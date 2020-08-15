/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

/**
 * CommitLog 文件存储目录为 ${ROCKET_HOME}/store/commitlog 目录
 * 每一个文件默认大小为 1G，一个文件写满以后，则创建另外一个，
 * 以该文件中第一个偏移量为文件名，偏移量小于20位用0补齐。
 * 这样可以快速定位到消息。
 *
 * MappedFileQueue 可以看作是 ${ROCKET_HOME}/store/commitlog 文件夹，
 * MappedFile 则对应该文件夹下一个个文件
 *
 * 是 RocketMQ 内存映射文件的具体实现。
 * RocketMQ 文件的一个组织方式是内存映射文件，预先申请一块连续的固定大小的内存，需要一套指针标识当前最大有效数据位置。
 * 在 MappedFile 设计中，只有提交了数据（写入到 MapperByteBuffer 或 FileChannel 中的数据）才是安全的数据
 */
public class MappedFile extends ReferenceResource {

    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    //操作系统每页大小，默认4K
    public static final int OS_PAGE_SIZE = 1024 * 4;

    //当前 JVM 实例中 MapperFile 虚拟内存
    private static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);

    //当前 JVM 实例中 MapperFile 对象个数
    private static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);

    //当前该文件的写指针，从 0 开始（内存映射文件中的写指针）
    protected final AtomicInteger wrotePosition = new AtomicInteger(0);

    //当前文件的提交指针，如果开启 transientStorePoolEnable，则数据会存储在 transientStorePool 中，
    //然后提交到内存映射 ByteBuffer 中，再刷写到磁盘
    protected final AtomicInteger committedPosition = new AtomicInteger(0);

    //刷写到磁盘指针，此指针之前的数据持久化到磁盘中
    private final AtomicInteger flushedPosition = new AtomicInteger(0);

    //文件大小
    protected int fileSize;

    //文件通道
    protected FileChannel fileChannel;

    //堆内存 ByteBuffer，
    //如果不为空，数据首先将存储在该 Buffer 中，
    //然后提交到 MapperFile 对应的内存映射文件 Buffer（mappedByteBuffer）。
    //transientStorePoolEnable 为 true 时不为空
    protected ByteBuffer writeBuffer = null;

    //物理文件对应的内存映射 Buffer
    private MappedByteBuffer mappedByteBuffer;

    //堆内存池。transientStorePoolEnable 为 true 时启用
    //短暂的存储池
    protected TransientStorePool transientStorePool = null;

    //文件名称
    private String fileName;

    //该文件的初始偏移量
    private long fileFromOffset;

    //物理文件
    private File file;

    //文件最后一次写入内容时间
    private volatile long storeTimestamp = 0;

    //是否是 MapperFileQueue 队列中第一个文件
    private boolean firstCreateInQueue = false;

    public MappedFile() {
    }

    public MappedFile(final String fileName, final int fileSize) throws IOException {
        init(fileName, fileSize);
    }

    public MappedFile(final String fileName, final int fileSize, final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize, transientStorePool);
    }

    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }

    public static void clean(final ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0)
            return;
        invoke(invoke(viewed(buffer), "cleaner"), "clean");
    }

    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        return AccessController.doPrivileged(new PrivilegedAction<Object>() {
            public Object run() {
                try {
                    Method method = method(target, methodName, args);
                    method.setAccessible(true);
                    return method.invoke(target);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        });
    }

    private static Method method(Object target, String methodName, Class<?>[] args)
        throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }

    private static ByteBuffer viewed(ByteBuffer buffer) {
        String methodName = "viewedBuffer";
        Method[] methods = buffer.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }

        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null)
            return buffer;
        else
            return viewed(viewedBuffer);
    }

    public static int getTotalMappedFiles() {
        return TOTAL_MAPPED_FILES.get();
    }

    public static long getTotalMappedVirtualMemory() {
        return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
    }

    /**
     * 根据是否开启 transientStorePoolEnable 存在两种初始化情况。
     *
     * transientStorePoolEnable 为 true 时，表示内容先存储在堆外内存{@link MappedFile#writeBuffer}，
     * 然后通过 Commit 线程将数据提交到内存映射 Buffer 中{@link MappedFile#mappedByteBuffer}，
     * 再通过 Flush 线程将内存映射 Buffer 中数据持久化到磁盘中
     *
     * @param fileName
     * @param fileSize
     * @param transientStorePool
     * @throws IOException
     */
    public void init(final String fileName, final int fileSize, final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize);

        //如果 transientStorePoolEnable 为 true，初始化 writeBuffer
        this.writeBuffer = transientStorePool.borrowBuffer();
        this.transientStorePool = transientStorePool;
    }

    /**
     * MapperFile 初始化
     * @param fileName
     * @param fileSize
     * @throws IOException
     */
    private void init(final String fileName, final int fileSize) throws IOException {
        //初始化 fileFromOffset 为文件名，也就是文件名代表该文件的起始偏移量
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;

        ensureDirOK(this.file.getParent());

        try {
            //通过 RandomAccessFile 创建读写文件通道
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();

            //将文件内容使用 NIO 的内存映射 Buffer，将文件映射到内存中
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);

            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
            TOTAL_MAPPED_FILES.incrementAndGet();
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("Failed to create file " + this.fileName, e);
            throw e;
        } catch (IOException e) {
            log.error("Failed to map file " + this.fileName, e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }

    public int getFileSize() {
        return fileSize;
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    //将消息追加到 mappedFile 中
    public AppendMessageResult appendMessage(final MessageExtBrokerInner msg, final AppendMessageCallback cb) {
        return appendMessagesInner(msg, cb);
    }

    public AppendMessageResult appendMessages(final MessageExtBatch messageExtBatch, final AppendMessageCallback cb) {
        return appendMessagesInner(messageExtBatch, cb);
    }

    //将消息追加到 mappedFile 中
    public AppendMessageResult appendMessagesInner(final MessageExt messageExt, final AppendMessageCallback appendMessageCallback) {
        assert messageExt != null;
        assert appendMessageCallback != null;

        //首先获取 MapperFile 当前写指针
        int currentPos = this.wrotePosition.get();

        //如果 currentPos 大于或等于文件大小则表明文件已经写满，抛出 AppendMessageStatus.UNKNOWN_ERROR

        //如果 currentPos 小于文件大小
        if (currentPos < this.fileSize) {
            //通过 slice() 方法创建一个与 MapperFile 的共享内存区
            ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : this.mappedByteBuffer.slice();
            //设置 position 为当前指针
            byteBuffer.position(currentPos);

            AppendMessageResult result;


            //开始追加消息
            if (messageExt instanceof MessageExtBrokerInner) {
                result = appendMessageCallback.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBrokerInner) messageExt);
            } else if (messageExt instanceof MessageExtBatch) {
                result = appendMessageCallback.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBatch) messageExt);
            } else {
                return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            }

            //更新消息队列逻辑偏移量
            this.wrotePosition.addAndGet(result.getWroteBytes());
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

    public boolean appendMessage(final byte[] data) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + data.length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(data.length);
            return true;
        }

        return false;
    }

    /**
     * Content of data from offset to offset + length will be wrote to file.
     *
     * @param offset The offset of the subarray to be used.
     * @param length The length of the subarray to be used.
     */
    public boolean appendMessage(final byte[] data, final int offset, final int length) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data, offset, length));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(length);
            return true;
        }

        return false;
    }

    /**
     * MapperFile刷盘
     * 刷盘是指将内存中的数据刷写到磁盘中，永久存储在磁盘中
     * @return The current flushed position
     */
    public int flush(final int flushLeastPages) {
        if (this.isAbleToFlush(flushLeastPages)) {
            if (this.hold()) {
                int value = getReadPosition();

                try {
                    //We only append data to fileChannel or mappedByteBuffer, never both.
                    //直接调用 force() 方法将内存中的数据持久化到磁盘中
                    if (writeBuffer != null || this.fileChannel.position() != 0) {
                        this.fileChannel.force(false);
                    } else {
                        this.mappedByteBuffer.force();
                    }
                } catch (Throwable e) {
                    log.error("Error occurred when force data to disk.", e);
                }

                this.flushedPosition.set(value);
                this.release();
            } else {
                log.warn("in flush, hold failed, flush offset = " + this.flushedPosition.get());
                this.flushedPosition.set(getReadPosition());
            }
        }
        return this.getFlushedPosition();
    }

    /**
     * 内存映射文件的提交动作
     *
     * @param commitLeastPages 本次提交的最小页数
     * @return
     */
    public int commit(final int commitLeastPages) {
        //writeBuffer 如果为空，直接返回 writeBuffer 指针，无需执行 commit 操作，表明 commit 操作主体是 writeBuffer
        if (writeBuffer == null) {
            //no need to commit data to file channel, so just regard wrotePosition as committedPosition.
            return this.wrotePosition.get();
        }

        //根据 commitLeastPages 判断是否需要执行提交操作
        if (this.isAbleToCommit(commitLeastPages)) {
            if (this.hold()) {
                commit0(commitLeastPages);
                this.release();
            } else {
                log.warn("in commit, hold failed, commit offset = " + this.committedPosition.get());
            }
        }

        // All dirty data has been committed to FileChannel.
        if (writeBuffer != null && this.transientStorePool != null && this.fileSize == this.committedPosition.get()) {
            this.transientStorePool.returnBuffer(writeBuffer);
            this.writeBuffer = null;
        }

        return this.committedPosition.get();
    }

    /**
     * 提交的具体实现
     *
     * 作用就是将 MapperFile 里 writeBuffer 中的数据提交到 FileChannel 中
     *
     * @param commitLeastPages
     */
    protected void commit0(final int commitLeastPages) {
        int writePos = this.wrotePosition.get();
        int lastCommittedPosition = this.committedPosition.get();

        if (writePos - this.committedPosition.get() > 0) {
            try {
                //创建一个 writeBuffer 共享子缓冲区
                //slice()创建一个共享缓冲区，与原先的ByteBuffer 共享内存，但维护一套独立的指针（position/mark/limit）
                ByteBuffer byteBuffer = writeBuffer.slice();
                //将 position 回退到上一次提交的位置committedPosition
                byteBuffer.position(lastCommittedPosition);
                //设置limit为 wrotePosition
                byteBuffer.limit(writePos);
                this.fileChannel.position(lastCommittedPosition);

                //把 committedPosition 到 wrotePosition 的数据复制（写入）到 FileChannel 中，
                this.fileChannel.write(byteBuffer);
                //更新 committedPosition 指针为 wrotePosition
                this.committedPosition.set(writePos);
            } catch (Throwable e) {
                log.error("Error occurred when commit data to FileChannel.", e);
            }
        }
    }

    private boolean isAbleToFlush(final int flushLeastPages) {
        int flush = this.flushedPosition.get();
        int write = getReadPosition();

        if (this.isFull()) {
            return true;
        }

        if (flushLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }

        return write > flush;
    }

    /**
     * 判断是否执行 commit 操作
     * @param commitLeastPages 本次提交的最小页数
     * @return
     */
    protected boolean isAbleToCommit(final int commitLeastPages) {
        //如果文件已经写满，返回 true
        if (this.isFull()) {
            return true;
        }

        int flush = this.committedPosition.get();
        int write = this.wrotePosition.get();

        //如果大于0
        if (commitLeastPages > 0) {
            //比较 wrotePosition（当前 writeBuffer的写指针）与上一次提交的指针（committedPosition）的差值，
            //除以 OS_PAGE_SIZE 得到当前脏页的数量，如果大于等于 commitLeastPages 则返回 true
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= commitLeastPages;
        }

        //如果 commitLeastPages 小于等于0，表示只要存储脏页就提交
        return write > flush;
    }

    public int getFlushedPosition() {
        return flushedPosition.get();
    }

    public void setFlushedPosition(int pos) {
        this.flushedPosition.set(pos);
    }

    public boolean isFull() {
        return this.fileSize == this.wrotePosition.get();
    }

    public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {
        int readPosition = getReadPosition();
        if ((pos + size) <= readPosition) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                    + this.fileFromOffset);
            }
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                + ", fileFromOffset: " + this.fileFromOffset);
        }

        return null;
    }

    public SelectMappedBufferResult selectMappedBuffer(int pos) {
        int readPosition = getReadPosition();
        if (pos < readPosition && pos >= 0) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                int size = readPosition - pos;
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }

        return null;
    }

    /**
     * 执行释放资源操作
     *
     * @param currentRef
     * @return
     */
    @Override
    public boolean cleanup(final long currentRef) {
        //如果available 为true，表示当前 MapperFile 当前可用，无需清理，返回false
        if (this.isAvailable()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have not shutdown, stop unmapping.");
            return false;
        }

        //如果资源被清理返回true
        if (this.isCleanupOver()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have cleanup, do not do it again.");
            return true;
        }

        //如果是堆外内存，调用堆外内存的cleanup方法清除，
        clean(this.mappedByteBuffer);

        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
        TOTAL_MAPPED_FILES.decrementAndGet();
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");

        return true;
    }

    /**
     * MapperFile 销毁
     *
     * 在整个过程中，首先是需要释放资源，释放资源的前提条件是，该MapperFile的引用小于等于0
     * 释放资源：
     * {@link ReferenceResource#release()}
     *
     * @param intervalForcibly 拒绝被销毁的最大存活时间
     * @return
     */
    public boolean destroy(final long intervalForcibly) {
        //关闭 MapperFile
        this.shutdown(intervalForcibly);

        //判断是否清理完成
        if (this.isCleanupOver()) {
            try {
                //关闭文件通道，删除物理文件
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();
                boolean result = this.file.delete();
                log.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName
                    + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePosition() + " M:"
                    + this.getFlushedPosition() + ", "
                    + UtilAll.computeElapsedTimeMilliseconds(beginTime));
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        } else {
            log.warn("destroy mapped file[REF:" + this.getRefCount() + "] " + this.fileName
                + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }

    public int getWrotePosition() {
        return wrotePosition.get();
    }

    public void setWrotePosition(int pos) {
        this.wrotePosition.set(pos);
    }

    /**
     * 获取 MapperFile 最大读指针
     * @return The max position which have valid data
     */
    public int getReadPosition() {
        //如果 writeBuffer 为空，则直接返回当前的写指针；如果不为空，则返回上一次提交的指针。
        return this.writeBuffer == null ? this.wrotePosition.get() : this.committedPosition.get();
    }

    public void setCommittedPosition(int pos) {
        this.committedPosition.set(pos);
    }

    public void warmMappedFile(FlushDiskType type, int pages) {
        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        int flush = 0;
        long time = System.currentTimeMillis();
        for (int i = 0, j = 0; i < this.fileSize; i += MappedFile.OS_PAGE_SIZE, j++) {
            byteBuffer.put(i, (byte) 0);
            // force flush when flush disk type is sync
            if (type == FlushDiskType.SYNC_FLUSH) {
                if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                    flush = i;
                    mappedByteBuffer.force();
                }
            }

            // prevent gc
            if (j % 1000 == 0) {
                log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                try {
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    log.error("Interrupted", e);
                }
            }
        }

        // force flush when prepare load finished
        if (type == FlushDiskType.SYNC_FLUSH) {
            log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}",
                this.getFileName(), System.currentTimeMillis() - beginTime);
            mappedByteBuffer.force();
        }
        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(),
            System.currentTimeMillis() - beginTime);

        this.mlock();
    }

    public String getFileName() {
        return fileName;
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }

    public void mlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {
            int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }

        {
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
            log.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
    }

    public void munlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

    //testable
    File getFile() {
        return this.file;
    }

    @Override
    public String toString() {
        return this.fileName;
    }
}
