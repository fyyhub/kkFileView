package cn.keking.service;

import cn.keking.config.ConfigConstants;
import cn.keking.utils.WebUtils;
import cn.keking.web.filter.BaseUrlFilter;
import com.itextpdf.text.Document;
import com.itextpdf.text.DocumentException;
import com.itextpdf.text.Image;
import com.itextpdf.text.io.FileChannelRandomAccessSource;
import com.itextpdf.text.pdf.PdfWriter;
import com.itextpdf.text.pdf.RandomAccessFileOrArray;
import com.itextpdf.text.pdf.codec.TiffImage;
import org.apache.commons.imaging.Imaging;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Component
public class TifToService {

    private static final int FIT_WIDTH = 500;
    private static final int FIT_HEIGHT = 900;
    private static final Logger logger = LoggerFactory.getLogger(TifToService.class);
    private static final String FILE_DIR = ConfigConstants.getFileDir();
    // 用于文档同步的锁对象
    private final Object documentLock = new Object();
    // 专用线程池用于TIF转换
    private static ExecutorService tifConversionPool;

    static {
        initThreadPool();
    }

    private static void initThreadPool() {
        int corePoolSize = getOptimalThreadCount();
        int maxPoolSize = corePoolSize * 2;
        long keepAliveTime = 60L;

        tifConversionPool = new ThreadPoolExecutor(
                corePoolSize,
                maxPoolSize,
                keepAliveTime,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(100),
                new ThreadFactory() {
                    private final AtomicInteger threadNumber = new AtomicInteger(1);

                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r);
                        t.setName("tif-convert-thread-" + threadNumber.getAndIncrement());
                        t.setDaemon(true);
                        return t;
                    }
                },
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

        logger.info("TIF转换线程池初始化完成，核心线程数: {}, 最大线程数: {}", corePoolSize, maxPoolSize);
    }

    private static int getOptimalThreadCount() {
        int cpuCores = Runtime.getRuntime().availableProcessors();
        // 对于I/O密集型任务，可以设置更多的线程
        return Math.min(cpuCores * 2, 16);
    }

    /**
     * 创建 RandomAccessFileOrArray 实例（修复已弃用的构造函数）
     */
    private RandomAccessFileOrArray createRandomAccessFileOrArray(File file) throws IOException {
        RandomAccessFile aFile = new RandomAccessFile(file, "r");
        FileChannel inChannel = aFile.getChannel();
        FileChannelRandomAccessSource fcra = new FileChannelRandomAccessSource(inChannel);
        return new RandomAccessFileOrArray(fcra);
    }

    /**
     * TIF转JPG - 支持多线程并行处理
     */
    public List<String> convertTif2Jpg(String strInputFile, String strOutputFile,
                                       boolean forceUpdatedCache) throws Exception {
        return convertTif2Jpg(strInputFile, strOutputFile, forceUpdatedCache, true);
    }

    /**
     * TIF转JPG - 可选择是否启用并行处理
     * @param parallelProcessing 是否启用并行处理
     */
    public List<String> convertTif2Jpg(String strInputFile, String strOutputFile,
                                       boolean forceUpdatedCache, boolean parallelProcessing) throws Exception {
        String baseUrl = BaseUrlFilter.getBaseUrl();
        String outputDirPath = strOutputFile.substring(0, strOutputFile.lastIndexOf('.'));

        File tiffFile = new File(strInputFile);
        if (!tiffFile.exists()) {
            logger.error("找不到文件【{}】", strInputFile);
            throw new FileNotFoundException("文件不存在: " + strInputFile);
        }

        File outputDir = new File(outputDirPath);
        if (!outputDir.exists() && !outputDir.mkdirs()) {
            throw new IOException("创建目录失败: " + outputDirPath);
        }

        // 加载所有图片
        List<BufferedImage> images;
        try {
            images = Imaging.getAllBufferedImages(tiffFile);
            logger.info("TIF文件加载完成，共{}页，文件: {}", images.size(), strInputFile);
        } catch (IOException e) {
            handleImagingException(e, strInputFile);
            throw e;
        }

        int pageCount = images.size();

        // 根据页面数量决定是否使用并行处理
        boolean useParallel = parallelProcessing && pageCount > 5;

        if (useParallel) {
            return convertParallel(images, outputDirPath, baseUrl, forceUpdatedCache);
        } else {
            return convertSequentially(images, outputDirPath, baseUrl, forceUpdatedCache);
        }
    }

    /**
     * 并行转换
     */
    private List<String> convertParallel(List<BufferedImage> images, String outputDirPath,
                                         String baseUrl, boolean forceUpdatedCache) {
        int pageCount = images.size();
        List<CompletableFuture<String>> futures = new ArrayList<>(pageCount);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger skipCount = new AtomicInteger(0);

        long startTime = System.currentTimeMillis();

        // 提交所有页面转换任务
        for (int i = 0; i < pageCount; i++) {
            final int pageIndex = i;
            BufferedImage image = images.get(i);

            CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> {
                try {
                    String fileName = outputDirPath + File.separator + pageIndex + ".jpg";
                    File outputFile = new File(fileName);

                    // 检查是否需要转换
                    if (forceUpdatedCache || !outputFile.exists()) {
                        // 使用PNG格式保持更好的质量，如果需要JPG可以调整
                        boolean success = ImageIO.write(image, "png", outputFile);
                        if (!success) {
                            logger.error("无法写入图片格式，页号: {}", pageIndex);
                            return null;
                        }
                        logger.debug("并行转换图片页 {} 完成", pageIndex);
                        successCount.incrementAndGet();
                    } else {
                        logger.debug("使用缓存图片页 {}", pageIndex);
                        skipCount.incrementAndGet();
                    }

                    // 构建URL
                    String relativePath = fileName.replace(FILE_DIR, "");
                    return baseUrl + WebUtils.encodeFileName(relativePath);

                } catch (Exception e) {
                    logger.error("并行转换页 {} 失败: {}", pageIndex, e.getMessage());
                    return null;
                }
            }, tifConversionPool);

            futures.add(future);
        }

        // 等待所有任务完成并收集结果
        List<String> imageUrls = futures.stream()
                .map(CompletableFuture::join)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        long elapsedTime = System.currentTimeMillis() - startTime;

        logger.info("TIF并行转换完成: 成功={}, 跳过={}, 总页数={}, 耗时={}ms",
                successCount.get(), skipCount.get(), pageCount, elapsedTime);

        return imageUrls;
    }

    /**
     * 串行转换
     */
    private List<String> convertSequentially(List<BufferedImage> images, String outputDirPath,
                                             String baseUrl, boolean forceUpdatedCache) throws Exception {
        List<String> imageUrls = new ArrayList<>(images.size());

        for (int i = 0; i < images.size(); i++) {
            String fileName = outputDirPath + File.separator + i + ".jpg";
            File outputFile = new File(fileName);

            try {
                if (forceUpdatedCache || !outputFile.exists()) {
                    BufferedImage image = images.get(i);
                    boolean success = ImageIO.write(image, "png", outputFile);
                    if (!success) {
                        throw new IOException("无法写入JPG格式图片: " + fileName);
                    }
                    logger.debug("转换图片页 {} 完成", i);
                } else {
                    logger.debug("使用缓存图片页 {}", i);
                }

                String relativePath = fileName.replace(FILE_DIR, "");
                String url = baseUrl + WebUtils.encodeFileName(relativePath);
                imageUrls.add(url);

            } catch (IOException e) {
                logger.error("转换页 {} 失败: {}", i, e.getMessage());
                throw e;
            }
        }

        return imageUrls;
    }

    /**
     * 将JPG图片转换为PDF - 优化版本
     */
    public void convertTif2Pdf(String strJpgFile, String strPdfFile) throws Exception {
        convertJpg2Pdf(strJpgFile, strPdfFile, true);
    }

    /**
     * 将JPG图片转换为PDF - 支持并行处理图片加载
     */
    public void convertJpg2Pdf(String strJpgFile, String strPdfFile, boolean parallelLoad) throws Exception {
        Document document = new Document();
        FileOutputStream outputStream = null;
        RandomAccessFileOrArray rafa = null;

        try {
            File tiffFile = new File(strJpgFile);

            // 修复：使用非弃用的方式创建 RandomAccessFileOrArray
            rafa = createRandomAccessFileOrArray(tiffFile);
            int pages = TiffImage.getNumberOfPages(rafa);
            logger.info("开始转换TIFF到PDF，总页数: {}", pages);

            outputStream = new FileOutputStream(strPdfFile);
            PdfWriter.getInstance(document, outputStream);
            document.open();

            // 修改为传入File对象而不是RandomAccessFileOrArray
            if (parallelLoad && pages > 10) {
                convertPagesParallel(document, tiffFile, pages);
            } else {
                convertPagesSequentially(document, tiffFile, pages);
            }

        } catch (IOException e) {
            handlePdfConversionException(e, strPdfFile);
            throw e;
        } finally {
            // 修复：传入 rafa 以正确关闭资源
            closeResources(document, rafa, outputStream);
        }

        logger.info("PDF转换完成: {}", strPdfFile);
    }

    /**
     * 串行处理页面 - 修复版
     */
    private void convertPagesSequentially(Document document, File tiffFile, int pages) throws IOException, DocumentException {
        RandomAccessFileOrArray rafa = null;
        try {
            // 修复：使用非弃用的方式创建 RandomAccessFileOrArray
            rafa = createRandomAccessFileOrArray(tiffFile);
            for (int i = 1; i <= pages; i++) {
                Image image = TiffImage.getTiffImage(rafa, i);
                image.scaleToFit(FIT_WIDTH, FIT_HEIGHT);
                document.add(image);

                if (i % 10 == 0) {
                    logger.debug("已处理 {} 页", i);
                }
            }
        } finally {
            if (rafa != null) {
                try {
                    rafa.close();
                } catch (Exception e) {
                    logger.warn("关闭RandomAccessFileOrArray失败", e);
                }
            }
        }
    }

    /**
     * 并行加载并添加图片到PDF - 修复版
     */
    private void convertPagesParallel(Document document, File tiffFile, int pages) {
        List<CompletableFuture<Image>> futures = new ArrayList<>();

        // 提交所有页面加载任务
        for (int i = 1; i <= pages; i++) {
            final int pageNum = i;
            CompletableFuture<Image> future = CompletableFuture.supplyAsync(() -> {
                RandomAccessFileOrArray localRafa = null;
                try {
                    // 为每个线程创建独立的RandomAccessFileOrArray
                    // 修复：使用非弃用的方式创建 RandomAccessFileOrArray
                    localRafa = createRandomAccessFileOrArray(tiffFile);

                    Image image = TiffImage.getTiffImage(localRafa, pageNum);
                    image.scaleToFit(FIT_WIDTH, FIT_HEIGHT);
                    logger.debug("并行加载TIFF页 {}", pageNum);
                    return image;
                } catch (Exception e) {
                    logger.error("加载TIFF页 {} 失败", pageNum, e);
                    return null;
                } finally {
                    if (localRafa != null) {
                        try {
                            localRafa.close();
                        } catch (Exception e) {
                            logger.warn("关闭RandomAccessFileOrArray失败", e);
                        }
                    }
                }
            }, tifConversionPool);

            futures.add(future);
        }
        // 按顺序添加到文档（保持页面顺序）
        for (int i = 0; i < futures.size(); i++) {
            try {
                Image image = futures.get(i).get(30, TimeUnit.SECONDS);
                if (image != null) {
                    // 使用专门的锁对象而不是同步document参数
                    synchronized (documentLock) {
                        document.add(image);
                    }
                }
            } catch (Exception e) {
                logger.error("添加页 {} 到PDF失败", i + 1, e);
            }
        }
    }

    /**
     * 异常处理
     */
    private void handleImagingException(IOException e, String filePath) {
        if (!e.getMessage().contains("Only sequential, baseline JPEGs are supported at the moment")) {
            logger.error("TIF转JPG异常，文件路径：{}", filePath, e);
        } else {
            logger.warn("不支持的非基线JPEG格式，文件：{}", filePath);
        }
    }

    private void handlePdfConversionException(IOException e, String filePath) {
        if (!e.getMessage().contains("Bad endianness tag (not 0x4949 or 0x4d4d)")) {
            logger.error("TIF转PDF异常，文件路径：{}", filePath, e);
        } else {
            logger.warn("TIFF文件字节顺序标记错误，文件：{}", filePath);
        }
    }

    /**
     * 资源关闭
     */
    private void closeResources(Document document, RandomAccessFileOrArray rafa, FileOutputStream outputStream) {
        try {
            if (document != null && document.isOpen()) {
                document.close();
            }
        } catch (Exception e) {
            logger.warn("关闭Document失败", e);
        }

        try {
            if (rafa != null) {
                rafa.close();
            }
        } catch (Exception e) {
            logger.warn("关闭RandomAccessFileOrArray失败", e);
        }

        try {
            if (outputStream != null) {
                outputStream.close();
            }
        } catch (Exception e) {
            logger.warn("关闭FileOutputStream失败", e);
        }
    }

    /**
     * 优雅关闭
     */
    public static void shutdown() {
        if (tifConversionPool != null && !tifConversionPool.isShutdown()) {
            tifConversionPool.shutdown();
            try {
                if (!tifConversionPool.awaitTermination(30, TimeUnit.SECONDS)) {
                    tifConversionPool.shutdownNow();
                }
            } catch (InterruptedException e) {
                tifConversionPool.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
}