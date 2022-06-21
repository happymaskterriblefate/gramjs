import { Api } from "../tl";

import { TelegramClient } from "./TelegramClient";
import { generateRandomBytes, readBigIntFromBuffer, sleep } from "../Helpers";
import { getAppropriatedPartSize, getInputMedia, getMessageId } from "../Utils";
import { EntityLike, FileLike, MarkupLike, MessageIDLike } from "../define";
import path from "./path";
import { promises as fs } from "fs";
import { Readable } from 'stream'
import { errors, utils } from "../index";
import { _parseMessageText } from "./messageParse";
import { getCommentData } from "./messages";
import bigInt from "big-integer";

interface OnProgress {
    // Float between 0 and 1.
    (progress: number): void;

    isCanceled?: boolean;
}

/**
 * interface for uploading files.
 */
export interface UploadFileParams {
    /** for browsers this should be an instance of File.<br/>
     * On node you should use {@link CustomFile} class to wrap your file.
     */
    file: File | CustomFile;
    /** How many workers to use to upload the file. anything above 16 is unstable. */
    workers: number;
    /** a progress callback for the upload. */
    onProgress?: OnProgress;
    maxBufferSize?: number;
}

interface UploadPart {
    /** Part number for this upload */
    partNumber: number;

    /** Buffer for this part */
    buffer: Buffer;
}
class UploadHandler {
    /** File to upload */
    file: File | CustomFile;

    /** id for this file */
    id: bigInt.BigInteger;

    /** size of chunks to be uploaded */
    partSize: number;

    /** number of chunks to be uploaded */
    partCount: number;

    /** Readable stream used to stream data for upload */
    stream: Readable | null;

    /** flag for when the stream has reached the end */
    done: boolean;

    /** Part number read */
    partNumber: number;

    /** Number of successfully upload */
    uploadedParts: number;

    /** Number of simultaneous upload jobs */
    workers: number;

    /** Number of bytes read so far */
    bytesRead: number;

    /** File size of file */
    filesize: number;

    /** Progress callback */
    onProgress?: OnProgress;

    /** is the file large */
    isLarge: boolean;

    constructor(file: File | CustomFile, workers?: number, onProgress?: OnProgress) {
        // Handle other types later
        this.file = file;
        this.id = readBigIntFromBuffer(generateRandomBytes(8), true, true);
        this.stream = null;
        this.partSize = 0;
        this.partCount = 0;
        this.done = false;
        this.isLarge = false;
        this.partNumber = 0;
        this.uploadedParts = 0;
        this.bytesRead = 0;
        this.filesize = 0;
        this.workers = workers || 1;
        this.onProgress = onProgress;
    }

    async setup() {
        const isBrowserFile = typeof File !== "undefined" && this.file instanceof File;
        // Browser
        if (isBrowserFile) {
            const fileBuffer = await (new Response(this.file as File).arrayBuffer() as Promise<Buffer>);
            this.filesize = fileBuffer.byteLength;
            this.stream = Readable.from(fileBuffer)
        // Node
        } else if (this.file instanceof CustomFile) {
            this.filesize = this.file.size;
        }

        this.partSize = getAppropriatedPartSize(bigInt(this.filesize)) * KB_TO_BYTES;
        this.partCount = Math.ceil(this.filesize  / this.partSize);
        this.isLarge = this.filesize > LARGE_FILE_THRESHOLD;
        this.workers = Math.min(this.workers, this.partCount);

        if (this.stream === null && this.file instanceof CustomFile) {
            // Buffer
            if (this.file.buffer !== undefined) {
                this.stream = Readable.from(this.file.buffer)

            // File system
            } else {
                this.stream = (await fs.open(this.file.path)).createReadStream({
                    highWaterMark: this.partSize * KB_TO_BYTES * 2
                });
            }
        }
    }

    async waitForReadableStream() {
        return new Promise((resolve) => this.stream?.once('readable', resolve));
    }

    async start(client: TelegramClient): Promise<Array<void>> {
        const worker = (): Promise<void> => {
            if (!this.done) {
                return this.uploadPart(client).then(worker)
            }
            return Promise.resolve();
        }

        await this.setup();

        return Promise.all(Array.from({ length: this.workers }).map(worker))
    }

    async uploadPart(client: TelegramClient, part?: UploadPart): Promise<void> {
        if (this.done) return;

        // We always upload from the DC we are in
        const sender = await client.getSender(
            client.session.dcId
        );

        part = part || await this.getChunk();

        if (!part.buffer) return;

        try {
            await sender.send(
                this.isLarge ?
                    new Api.upload.SaveBigFilePart({
                            fileId: this.id,
                            filePart: part.partNumber,
                            fileTotalParts: this.partCount,
                            bytes: part.buffer,
                    }) :
                    new Api.upload.SaveFilePart({
                            fileId: this.id,
                            filePart: part.partNumber,
                            bytes: part.buffer,
                        })
            );

            this.uploadedParts += 1;

            if (this.onProgress) {
                this.onProgress(this.uploadedParts / this.partCount)
            }
        } catch (err) {
            if (sender && !sender.isConnected()) {
                await sleep(DISCONNECT_SLEEP);
                return this.uploadPart(client, part);
            } else if (err instanceof errors.FloodWaitError) {
                await sleep(err.seconds * 1000);
                return this.uploadPart(client, part);
            }
            throw err;
        }
    }

    async getChunk(): Promise<UploadPart> {
        let buffer;
        if (this.filesize - this.bytesRead > this.partSize) {
            buffer = await this.stream?.read(this.partSize);
        } else {
            buffer = await this.stream?.read();
        }

        if (buffer) {
            this.bytesRead += buffer.byteLength;
            this.done = this.bytesRead >= this.filesize;

            return {
                partNumber: this.partNumber++,
                buffer
            }
        }

        await sleep(100);
        return this.getChunk();
    }

}

/**
 * A custom file class that mimics the browser's File class.<br/>
 * You should use this whenever you want to upload a file.
 */
export class CustomFile {
    /** The name of the file to be uploaded. This is what will be shown in telegram */
    name: string;
    /** The size of the file. this should be the exact size to not lose any information */
    size: number;
    /** The full path on the system to where the file is. this will be used to read the file from.<br/>
     * Can be left empty to use a buffer instead
     */
    path: string;
    /** in case of the no path a buffer can instead be passed instead to upload. */
    buffer?: Buffer;


    constructor(name: string, size: number, path: string, buffer?: Buffer) {
        this.name = name;
        this.size = size;
        this.path = path;
        this.buffer = buffer;
    }
}

interface CustomBufferOptions {
    filePath?: string;
    buffer?: Buffer;
}

class CustomBuffer {
    constructor(private readonly options: CustomBufferOptions) {
        if (!options.buffer && !options.filePath) {
            throw new Error(
                "Either one of `buffer` or `filePath` should be specified"
            );
        }
    }

    async slice(begin: number, end: number): Promise<Buffer> {
        const { buffer, filePath } = this.options;
        if (buffer) {
            return buffer.slice(begin, end);
        } else if (filePath) {
            const buffSize = end - begin;
            const buff = Buffer.alloc(buffSize);
            const fHandle = await fs.open(filePath, "r");

            await fHandle.read(buff, 0, buffSize, begin);
            await fHandle.close();

            return Buffer.from(buff);
        }

        return Buffer.alloc(0);
    }
}

const KB_TO_BYTES = 1024;
const LARGE_FILE_THRESHOLD = 10 * 1024 * 1024;
const DISCONNECT_SLEEP = 1000;


/** @hidden */
export async function uploadFile(
    client: TelegramClient,
    fileParams: UploadFileParams
): Promise<Api.InputFile | Api.InputFileBig> {
    const { file, workers, onProgress } = fileParams;
    const { name, size } = file;
    const uploadHandler = new UploadHandler(file, workers, onProgress);
    const isLarge = size > LARGE_FILE_THRESHOLD;

    // Make sure a new sender can be created before starting upload
    await client.getSender(client.session.dcId);
    await uploadHandler.start(client);

    return isLarge
        ? new Api.InputFileBig({
            id: uploadHandler.id,
            parts: uploadHandler.partCount,
            name,
        })
        : new Api.InputFile({
            id: uploadHandler.id,
            parts: uploadHandler.partCount,
            name,
            md5Checksum: "", // This is not a "flag", so not sure if we can make it optional.
        });
}

/**
 * Interface for sending files to a chat.
 */
export interface SendFileInterface {
    /** a file like object.
     *   - can be a localpath. the file name will be used.
     *   - can be a Buffer with a ".name" attribute to use as the file name.
     *   - can be an external direct URL. Telegram will download the file and send it.
     *   - can be an existing media from another message.
     *   - can be a handle to a file that was received by using {@link uploadFile}
     *   - can be a list when using an album
     *   - can be {@link Api.TypeInputMedia} instance. For example if you want to send a dice you would use {@link Api.InputMediaDice}
     */
    file: FileLike | FileLike[];
    /** Optional caption for the sent media message. can be a list for albums*/
    caption?: string | string[];
    /** If left to false and the file is a path that ends with the extension of an image file or a video file, it will be sent as such. Otherwise always as a document. */
    forceDocument?: boolean;
    /** The size of the file to be uploaded if it needs to be uploaded, which will be determined automatically if not specified. */
    fileSize?: number;
    /** Whether the existing draft should be cleared or not. */
    clearDraft?: boolean;
    /** progress callback that will be called each time a new chunk is downloaded. */
    progressCallback?: OnProgress;
    /** Same as `replyTo` from {@link sendMessage}. */
    replyTo?: MessageIDLike;
    /** Optional attributes that override the inferred ones, like {@link Api.DocumentAttributeFilename} and so on.*/
    attributes?: Api.TypeDocumentAttribute[] | Api.TypeDocumentAttribute[][];
    /** Optional JPEG thumbnail (for documents). Telegram will ignore this parameter unless you pass a .jpg file!<br/>
     * The file must also be small in dimensions and in disk size. Successful thumbnails were files below 20kB and 320x320px.<br/>
     *  Width/height and dimensions/size ratios may be important.
     *  For Telegram to accept a thumbnail, you must provide the dimensions of the underlying media through `attributes:` with DocumentAttributesVideo.
     */
    thumb?: FileLike;
    /** If true the audio will be sent as a voice note. */
    voiceNote?: boolean;
    /** If true the video will be sent as a video note, also known as a round video message.*/
    videoNote?: boolean;
    /** Whether the sent video supports streaming or not.<br/>
     *  Note that Telegram only recognizes as streamable some formats like MP4, and others like AVI or MKV will not work.<br/>
     *  You should convert these to MP4 before sending if you want them to be streamable. Unsupported formats will result in VideoContentTypeError. */
    supportsStreaming?: boolean;
    /** See the {@link parseMode} property for allowed values. Markdown parsing will be used by default. */
    parseMode?: any;
    /** A list of message formatting entities. When provided, the parseMode is ignored. */
    formattingEntities?: Api.TypeMessageEntity[];
    /** Whether the message should notify people in a broadcast channel or not. Defaults to false, which means it will notify them. Set it to True to alter this behaviour. */
    silent?: boolean;
    /**
     * If set, the file won't send immediately, and instead it will be scheduled to be automatically sent at a later time.
     */
    scheduleDate?: number;
    /**
     * The matrix (list of lists), row list or button to be shown after sending the message.<br/>
     * This parameter will only work if you have signed in as a bot. You can also pass your own ReplyMarkup here.
     */
    buttons?: MarkupLike;
    /** How many workers to use to upload the file. anything above 16 is unstable. */
    workers?: number;
    noforwards?: boolean;
    /** Similar to ``replyTo``, but replies in the linked group of a broadcast channel instead (effectively leaving a "comment to" the specified message).

     This parameter takes precedence over ``replyTo``.
     If there is no linked chat, `SG_ID_INVALID` is thrown.
     */
    commentTo?: number | Api.Message;
    /**
     * Used for threads to reply to a specific thread
     */
    topMsgId?: number | Api.Message;
}

interface FileToMediaInterface {
    file: FileLike;
    forceDocument?: boolean;
    fileSize?: number;
    progressCallback?: OnProgress;
    attributes?: Api.TypeDocumentAttribute[];
    thumb?: FileLike;
    voiceNote?: boolean;
    videoNote?: boolean;
    supportsStreaming?: boolean;
    mimeType?: string;
    asImage?: boolean;
    workers?: number;
}

/** @hidden */
export async function _fileToMedia(
    client: TelegramClient,
    {
        file,
        forceDocument,
        fileSize,
        progressCallback,
        attributes,
        thumb,
        voiceNote = false,
        videoNote = false,
        supportsStreaming = false,
        mimeType,
        asImage,
        workers = 1,
    }: FileToMediaInterface
): Promise<{
    fileHandle?: any;
    media?: Api.TypeInputMedia;
    image?: boolean;
}> {
    if (!file) {
        return { fileHandle: undefined, media: undefined, image: undefined };
    }
    const isImage = utils.isImage(file);

    if (asImage == undefined) {
        asImage = isImage && !forceDocument;
    }
    if (
        typeof file == "object" &&
        !Buffer.isBuffer(file) &&
        !(file instanceof Api.InputFile) &&
        !(file instanceof Api.InputFileBig) &&
        !(file instanceof CustomFile) &&
        !("read" in file)
    ) {
        try {
            return {
                fileHandle: undefined,
                media: utils.getInputMedia(file, {
                    isPhoto: asImage,
                    attributes: attributes,
                    forceDocument: forceDocument,
                    voiceNote: voiceNote,
                    videoNote: videoNote,
                    supportsStreaming: supportsStreaming,
                }),
                image: asImage,
            };
        } catch (e) {
            return {
                fileHandle: undefined,
                media: undefined,
                image: isImage,
            };
        }
    }
    let media;
    let fileHandle;
    let createdFile;

    if (file instanceof Api.InputFile || file instanceof Api.InputFileBig) {
        fileHandle = file;
    } else if (
        typeof file == "string" &&
        (file.startsWith("https://") || file.startsWith("http://"))
    ) {
        if (asImage) {
            media = new Api.InputMediaPhotoExternal({ url: file });
        } else {
            media = new Api.InputMediaDocumentExternal({ url: file });
        }
    } else if (!(typeof file == "string") || (await fs.lstat(file)).isFile()) {
        if (typeof file == "string") {
            createdFile = new CustomFile(
                path.basename(file),
                (await fs.stat(file)).size,
                file
            );
        } else if (
            (typeof File !== "undefined" && file instanceof File) ||
            file instanceof CustomFile
        ) {
            createdFile = file;
        } else {
            let name;
            if ("name" in file) {
                // @ts-ignore
                name = file.name;
            } else {
                name = "unnamed";
            }
            if (Buffer.isBuffer(file)) {
                createdFile = new CustomFile(name, file.length, "", file);
            }
        }
        if (!createdFile) {
            throw new Error(
                `Could not create file from ${JSON.stringify(file)}`
            );
        }
        fileHandle = await uploadFile(client, {
            file: createdFile,
            onProgress: progressCallback,
            workers: workers,
        });
    } else {
        throw new Error(`"Not a valid path nor a url ${file}`);
    }
    if (media != undefined) {
    } else if (fileHandle == undefined) {
        throw new Error(
            `Failed to convert ${file} to media. Not an existing file or an HTTP URL`
        );
    } else if (asImage) {
        media = new Api.InputMediaUploadedPhoto({
            file: fileHandle,
        });
    } else {
        // @ts-ignore
        let res = utils.getAttributes(file, {
            mimeType: mimeType,
            attributes: attributes,
            forceDocument: forceDocument && !isImage,
            voiceNote: voiceNote,
            videoNote: videoNote,
            supportsStreaming: supportsStreaming,
            thumb: thumb,
        });
        attributes = res.attrs;
        mimeType = res.mimeType;

        let uploadedThumb;
        if (!thumb) {
            uploadedThumb = undefined;
        } else {
            // todo refactor
            if (typeof thumb == "string") {
                uploadedThumb = new CustomFile(
                    path.basename(thumb),
                    (await fs.stat(thumb)).size,
                    thumb
                );
            } else if (typeof File !== "undefined" && thumb instanceof File) {
                uploadedThumb = thumb;
            } else {
                let name;
                if ("name" in thumb) {
                    name = thumb.name;
                } else {
                    name = "unnamed";
                }
                if (Buffer.isBuffer(thumb)) {
                    uploadedThumb = new CustomFile(
                        name,
                        thumb.length,
                        "",
                        thumb
                    );
                }
            }
            if (!uploadedThumb) {
                throw new Error(`Could not create file from ${file}`);
            }
            uploadedThumb = await uploadFile(client, {
                file: uploadedThumb,
                workers: 1,
            });
        }
        media = new Api.InputMediaUploadedDocument({
            file: fileHandle,
            mimeType: mimeType,
            attributes: attributes,
            thumb: uploadedThumb,
            forceFile: forceDocument && !isImage,
        });
    }
    return {
        fileHandle: fileHandle,
        media: media,
        image: asImage,
    };
}

/** @hidden */
export async function _sendAlbum(
    client: TelegramClient,
    entity: EntityLike,
    {
        file,
        caption,
        forceDocument = false,
        fileSize,
        clearDraft = false,
        progressCallback,
        replyTo,
        attributes,
        thumb,
        parseMode,
        voiceNote = false,
        videoNote = false,
        silent,
        supportsStreaming = false,
        scheduleDate,
        workers = 1,
        noforwards,
        commentTo,
        topMsgId,
    }: SendFileInterface
) {
    entity = await client.getInputEntity(entity);
    let files = [];
    if (!Array.isArray(file)) {
        files = [file];
    } else {
        files = file;
    }
    if (!Array.isArray(caption)) {
        if (!caption) {
            caption = "";
        }
        caption = [caption];
    }
    const captions: [string, Api.TypeMessageEntity[]][] = [];
    for (const c of caption) {
        captions.push(await _parseMessageText(client, c, parseMode));
    }
    if (commentTo != undefined) {
        const discussionData = await getCommentData(client, entity, commentTo);
        entity = discussionData.entity;
        replyTo = discussionData.replyTo;
    } else {
        replyTo = utils.getMessageId(replyTo);
    }
    if (!attributes) {
        attributes = [];
    }

    let index = 0;
    const albumFiles = [];
    for (const file of files) {
        let { fileHandle, media, image } = await _fileToMedia(client, {
            file: file,
            forceDocument: forceDocument,
            fileSize: fileSize,
            progressCallback: progressCallback,
            // @ts-ignore
            attributes: attributes[index],
            thumb: thumb,
            voiceNote: voiceNote,
            videoNote: videoNote,
            supportsStreaming: supportsStreaming,
            workers: workers,
        });
        index++;
        if (
            media instanceof Api.InputMediaUploadedPhoto ||
            media instanceof Api.InputMediaPhotoExternal
        ) {
            const r = await client.invoke(
                new Api.messages.UploadMedia({
                    peer: entity,
                    media,
                })
            );
            if (r instanceof Api.MessageMediaPhoto) {
                media = getInputMedia(r.photo);
            }
        } else if (media instanceof Api.InputMediaUploadedDocument) {
            const r = await client.invoke(
                new Api.messages.UploadMedia({
                    peer: entity,
                    media,
                })
            );
            if (r instanceof Api.MessageMediaDocument) {
                media = getInputMedia(r.document);
            }
        }
        let text = "";
        let msgEntities: Api.TypeMessageEntity[] = [];
        if (captions.length) {
            [text, msgEntities] = captions.shift()!;
        }
        albumFiles.push(
            new Api.InputSingleMedia({
                media: media!,
                message: text,
                entities: msgEntities,
            })
        );
    }
    let replyObject = undefined;
    if (replyTo != undefined) {
        replyObject = new Api.InputReplyToMessage({
            replyToMsgId: getMessageId(replyTo)!,
            topMsgId: getMessageId(topMsgId),
        });
    }

    const result = await client.invoke(
        new Api.messages.SendMultiMedia({
            peer: entity,
            replyTo: replyObject,
            multiMedia: albumFiles,
            silent: silent,
            scheduleDate: scheduleDate,
            clearDraft: clearDraft,
            noforwards: noforwards,
        })
    );
    const randomIds = albumFiles.map((m) => m.randomId);
    return client._getResponseMessage(randomIds, result, entity) as Api.Message;
}

/** @hidden */
export async function sendFile(
    client: TelegramClient,
    entity: EntityLike,
    {
        file,
        caption,
        forceDocument = false,
        fileSize,
        clearDraft = false,
        progressCallback,
        replyTo,
        attributes,
        thumb,
        parseMode,
        formattingEntities,
        voiceNote = false,
        videoNote = false,
        buttons,
        silent,
        supportsStreaming = false,
        scheduleDate,
        workers = 1,
        noforwards,
        commentTo,
        topMsgId,
    }: SendFileInterface
) {
    if (!file) {
        throw new Error("You need to specify a file");
    }
    if (!caption) {
        caption = "";
    }
    entity = await client.getInputEntity(entity);
    if (commentTo != undefined) {
        const discussionData = await getCommentData(client, entity, commentTo);
        entity = discussionData.entity;
        replyTo = discussionData.replyTo;
    } else {
        replyTo = utils.getMessageId(replyTo);
    }
    if (Array.isArray(file)) {
        return await _sendAlbum(client, entity, {
            file: file,
            caption: caption,
            replyTo: replyTo,
            parseMode: parseMode,
            attributes: attributes,
            silent: silent,
            scheduleDate: scheduleDate,
            supportsStreaming: supportsStreaming,
            clearDraft: clearDraft,
            forceDocument: forceDocument,
            noforwards: noforwards,
            topMsgId: topMsgId,
        });
    }
    if (Array.isArray(caption)) {
        caption = caption[0] || "";
    }
    let msgEntities;
    if (formattingEntities != undefined) {
        msgEntities = formattingEntities;
    } else {
        [caption, msgEntities] = await _parseMessageText(
            client,
            caption,
            parseMode
        );
    }

    const { fileHandle, media, image } = await _fileToMedia(client, {
        file: file,
        forceDocument: forceDocument,
        fileSize: fileSize,
        progressCallback: progressCallback,
        // @ts-ignore
        attributes: attributes,
        thumb: thumb,
        voiceNote: voiceNote,
        videoNote: videoNote,
        supportsStreaming: supportsStreaming,
        workers: workers,
    });
    if (media == undefined) {
        throw new Error(`Cannot use ${file} as file.`);
    }
    const markup = client.buildReplyMarkup(buttons);
    let replyObject = undefined;
    if (replyTo != undefined) {
        replyObject = new Api.InputReplyToMessage({
            replyToMsgId: getMessageId(replyTo)!,
            topMsgId: getMessageId(topMsgId),
        });
    }

    const request = new Api.messages.SendMedia({
        peer: entity,
        media: media,
        replyTo: replyObject,
        message: caption,
        entities: msgEntities,
        replyMarkup: markup,
        silent: silent,
        scheduleDate: scheduleDate,
        clearDraft: clearDraft,
        noforwards: noforwards,
    });
    const result = await client.invoke(request);
    return client._getResponseMessage(request, result, entity) as Api.Message;
}
