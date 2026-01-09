package cn.keking.service;

import cn.keking.config.ConfigConstants;
import cn.keking.model.FileAttribute;
import cn.keking.service.cache.NotResourceCache;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.pdfbox.Loader;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.ImageType;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.tools.imageio.ImageIOUtil;
import org.apache.poi.EncryptedDocumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author yudian-it
 */
@Component
public class PdfToJpgService {
    private final FileHandlerService fileHandlerService;

    // PDF转换专用线程池
    private ExecutorService pdfConversionPool;
    private ThreadPoolExecutor pdfThreadPoolExecutor;

    private static final Logger logger = LoggerFactory.getLogger(PdfToJpgService.class);
    private static final String PDF_PASSWORD_MSG = "password";
    private static final String PDF2JPG_IMAGE_FORMAT = ".jpg";

    // 最大并行页数阈值
    private static final int MAX_PARALLEL_PAGES = 20;

    public PdfToJpgService(FileHandlerService fileHandlerService) {
        this.fileHandlerService = fileHandlerService;
    }

    @PostConstruct
    public void init() {
        try {
            int threadCount = getPdfThreadPoolSize();
            int queueCapacity = threadCount * 10;

            AtomicInteger threadNum = new AtomicInteger(1);
            pdfThreadPoolExecutor = new ThreadPoolExecutor(
                    threadCount,
                    threadCount,
                    60L,
                    TimeUnit.SECONDS,
                    new LinkedBlockingQueue<>(queueCapacity),
                    r -> {
                        Thread t = new Thread(r);
                        t.setName("pdf-convert-pool-" + threadNum.getAndIncrement());
                        t.setUncaughtExceptionHandler((thread, throwable) ->
                                logger.error("PDF转换线程未捕获异常: {}", thread.getName(), throwable));
                        return t;
                    },
                    new ThreadPoolExecutor.CallerRunsPolicy()
            );

            pdfThreadPoolExecutor.allowCoreThreadTimeOut(true);
            pdfConversionPool = pdfThreadPoolExecutor;

            logger.info("PDF转换线程池初始化完成，线程数: {}, 队列容量: {}",
                    threadCount, queueCapacity);

        } catch (Exception e) {
            logger.error("PDF转换线程池初始化失败", e);
            int defaultThreads = Math.max(1, Runtime.getRuntime().availableProcessors() / 2);
            pdfConversionPool = Executors.newFixedThreadPool(defaultThreads);
            logger.warn("使用默认PDF线程池配置，线程数: {}", defaultThreads);
        }
    }

    private int getPdfThreadPoolSize() {
        try {
            String pdfThreadConfig = System.getProperty("pdf.thread.count");
            int threadCount;
            if (pdfThreadConfig != null) {
                threadCount = Integer.parseInt(pdfThreadConfig);
            } else {
                threadCount = Math.max(1, Runtime.getRuntime().availableProcessors() / 2);
            }

            if (threadCount <= 0) {
                threadCount = Runtime.getRuntime().availableProcessors();
                logger.warn("PDF线程数配置无效，使用CPU核心数: {}", threadCount);
            }

            int maxThreads = Runtime.getRuntime().availableProcessors() * 2;
            if (threadCount > maxThreads) {
                logger.warn("PDF线程数配置过大({})，限制为: {}", threadCount, maxThreads);
                threadCount = maxThreads;
            }
            return threadCount;
        } catch (Exception e) {
            logger.error("获取PDF线程数配置失败", e);
            return Math.max(1, Runtime.getRuntime().availableProcessors() / 2);
        }
    }

    @PreDestroy
    public void shutdown() {
        if (pdfConversionPool != null && !pdfConversionPool.isShutdown()) {
            gracefulShutdown(pdfConversionPool, getShutdownTimeout());
        }
    }

    private long getShutdownTimeout() {
        try {
            String pdfTimeout = System.getProperty("pdf.timeout");
            if (pdfTimeout != null) {
                return Long.parseLong(pdfTimeout);
            }
            return Long.parseLong(ConfigConstants.getCadTimeout());
        } catch (Exception e) {
            logger.warn("获取PDF关闭超时时间失败，使用默认值60秒", e);
            return 60L;
        }
    }

    private void gracefulShutdown(ExecutorService executor, long timeoutSeconds) {
        logger.info("开始关闭{}...", "PDF转换线程池");
        executor.shutdown();

        try {
            if (!executor.awaitTermination(timeoutSeconds, TimeUnit.SECONDS)) {
                logger.warn("{}超时未关闭，尝试强制关闭...", "PDF转换线程池");
                executor.shutdownNow();

                if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                    logger.error("{}无法完全关闭", "PDF转换线程池");
                } else {
                    logger.info("{}已强制关闭", "PDF转换线程池");
                }
            } else {
                logger.info("{}已正常关闭", "PDF转换线程池");
            }
        } catch (InterruptedException e) {
            logger.error("{}关闭时被中断", "PDF转换线程池", e);
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public List<String> pdf2jpg(String fileNameFilePath, String pdfFilePath,
                                String pdfName, FileAttribute fileAttribute) throws Exception {
        boolean forceUpdatedCache = fileAttribute.forceUpdatedCache();
        boolean usePasswordCache = fileAttribute.getUsePasswordCache();
        String filePassword = fileAttribute.getFilePassword();

        // 1. 检查缓存
        if (!forceUpdatedCache) {
            List<String> cacheResult = fileHandlerService.loadPdf2jpgCache(pdfFilePath);
            if (!CollectionUtils.isEmpty(cacheResult)) {
                return cacheResult;
            }
        }

        // 2. 验证文件存在
        File pdfFile = new File(fileNameFilePath);
        if (!pdfFile.exists()) {
            logger.error("PDF文件不存在: {}", fileNameFilePath);
            return null;
        }

        // 3. 创建输出目录
        int index = pdfFilePath.lastIndexOf(".");
        String folder = pdfFilePath.substring(0, index);
        File path = new File(folder);
        if (!path.exists() && !path.mkdirs()) {
            logger.error("创建转换文件目录失败: {}", folder);
            throw new IOException("无法创建输出目录");
        }

        // 4. 加载PDF文档获取页数
        int pageCount = 0;
        try (PDDocument tempDoc = Loader.loadPDF(pdfFile, filePassword)) {
            pageCount = tempDoc.getNumberOfPages();
            logger.info("PDF文件总页数: {}, 文件: {}", pageCount, pdfFilePath);
        } catch (IOException e) {
            handlePdfLoadException(e, pdfFilePath);
            throw new Exception("PDF文件加载失败", e);
        }

        // 5. 根据页数决定转换策略
        List<String> imageUrls;
        if (pageCount > MAX_PARALLEL_PAGES) {
            // 大文件使用新方案：每页独立加载PDF
            imageUrls = convertParallelIndependent(pdfFile, filePassword, pdfFilePath, folder, pageCount);
        } else {
            // 小文件使用串行处理（稳定）
            imageUrls = convertSequentially(pdfFile, filePassword, pdfFilePath, folder, pageCount);
        }

        // 6. 缓存结果
        if (usePasswordCache || ObjectUtils.isEmpty(filePassword)) {
            fileHandlerService.addPdf2jpgCache(pdfFilePath, pageCount);
        }

        logger.info("PDF转换完成，成功转换{}页，文件: {}", imageUrls.size(), pdfFilePath);
        return imageUrls;
    }

    /**
     * 处理PDF加载异常
     */
    private void handlePdfLoadException(Exception e, String pdfFilePath) throws Exception {
        Throwable[] throwableArray = ExceptionUtils.getThrowables(e);
        for (Throwable throwable : throwableArray) {
            if (throwable instanceof IOException || throwable instanceof EncryptedDocumentException) {
                if (e.getMessage().toLowerCase().contains(PDF_PASSWORD_MSG)) {
                    logger.info("PDF文件需要密码: {}", pdfFilePath);
                    throw new Exception(PDF_PASSWORD_MSG, e);
                }
            }
        }
        logger.error("加载PDF文件异常, pdfFilePath：{}", pdfFilePath, e);
        throw new Exception("PDF文件加载失败", e);
    }

    /**
     * 串行转换（稳定方案）
     */
    private List<String> convertSequentially(File pdfFile, String filePassword,
                                             String pdfFilePath, String folder, int pageCount) {
        List<String> imageUrls = new ArrayList<>(pageCount);

        try (PDDocument doc = Loader.loadPDF(pdfFile, filePassword)) {
            doc.setResourceCache(new NotResourceCache());
            PDFRenderer pdfRenderer = new PDFRenderer(doc);
            pdfRenderer.setSubsamplingAllowed(true);

            for (int pageIndex = 0; pageIndex < pageCount; pageIndex++) {
                try {
                    String imageFilePath = folder + File.separator + pageIndex + PDF2JPG_IMAGE_FORMAT;
                    BufferedImage image = pdfRenderer.renderImageWithDPI(
                            pageIndex,
                            ConfigConstants.getPdf2JpgDpi(),
                            ImageType.RGB
                    );

                    ImageIOUtil.writeImage(image, imageFilePath, ConfigConstants.getPdf2JpgDpi());
                    image.flush();

                    String imageUrl = fileHandlerService.getPdf2jpgUrl(pdfFilePath, pageIndex);
                    imageUrls.add(imageUrl);

                    logger.debug("串行转换页 {} 完成", pageIndex);

                } catch (Exception e) {
                    logger.error("串行转换页 {} 失败: {}", pageIndex, e.getMessage());
                }
            }
        } catch (Exception e) {
            logger.error("串行转换PDF失败", e);
        }

        return imageUrls;
    }

    /**
     * 并行转换 - 每个线程独立加载PDF（避免线程安全问题）
     */
    private List<String> convertParallelIndependent(File pdfFile, String filePassword,
                                                    String pdfFilePath, String folder, int pageCount) {
        List<String> imageUrls = Collections.synchronizedList(new ArrayList<>());
        List<Future<Boolean>> futures = new ArrayList<>();
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);

        // 提交页面转换任务
        for (int pageIndex = 0; pageIndex < pageCount; pageIndex++) {
            final int currentPage = pageIndex;

            Future<Boolean> future = pdfConversionPool.submit(() -> {
                try {
                    // 每个任务独立加载PDF，确保线程安全
                    try (PDDocument pageDoc = Loader.loadPDF(pdfFile, filePassword)) {
                        pageDoc.setResourceCache(new NotResourceCache());
                        PDFRenderer renderer = new PDFRenderer(pageDoc);
                        renderer.setSubsamplingAllowed(true);

                        String imageFilePath = folder + File.separator + currentPage + PDF2JPG_IMAGE_FORMAT;
                        BufferedImage image = renderer.renderImageWithDPI(
                                currentPage,
                                ConfigConstants.getPdf2JpgDpi(),
                                ImageType.RGB
                        );

                        ImageIOUtil.writeImage(image, imageFilePath, ConfigConstants.getPdf2JpgDpi());
                        image.flush();

                        String imageUrl = fileHandlerService.getPdf2jpgUrl(pdfFilePath, currentPage);
                        synchronized (imageUrls) {
                            imageUrls.add(imageUrl);
                        }

                        successCount.incrementAndGet();
                        logger.debug("并行转换页 {} 完成", currentPage);
                        return true;
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    logger.error("并行转换页 {} 失败: {}", currentPage, e.getMessage());
                    return false;
                }
            });

            futures.add(future);
        }

        // 等待所有任务完成
        int timeout = calculateTimeout(pageCount);
        long startTime = System.currentTimeMillis();

        for (Future<Boolean> future : futures) {
            try {
                future.get(timeout, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                logger.warn("页面转换任务超时，取消剩余任务");
                for (Future<Boolean> f : futures) {
                    if (!f.isDone()) {
                        f.cancel(true);
                    }
                }
                break;
            } catch (Exception e) {
                logger.error("页面转换任务执行失败", e);
            }

            // 检查是否超时
            if (System.currentTimeMillis() - startTime > timeout * 1000L) {
                logger.warn("PDF转换整体超时，取消剩余任务");
                for (Future<Boolean> f : futures) {
                    if (!f.isDone()) {
                        f.cancel(true);
                    }
                }
                break;
            }
        }

        long elapsedTime = System.currentTimeMillis() - startTime;
        logger.info("并行转换统计: 成功={}, 失败={}, 总页数={}, 耗时={}ms",
                successCount.get(), errorCount.get(), pageCount, elapsedTime);

        // 按页码排序
        imageUrls.sort(Comparator.comparingInt(url -> {
            try {
                String pageStr = url.substring(url.lastIndexOf('/') + 1, url.lastIndexOf('.'));
                return Integer.parseInt(pageStr);
            } catch (Exception e) {
                return 0;
            }
        }));

        return imageUrls;
    }

    /**
     * 计算超时时间
     */
    private int calculateTimeout(int pageCount) {
        if (pageCount <= 50) {
            return ConfigConstants.getPdfTimeout();
        } else if (pageCount <= 200) {
            return ConfigConstants.getPdfTimeout80();
        } else {
            return ConfigConstants.getPdfTimeout200();
        }
    }

    /**
     * 监控线程池状态
     */
    public void monitorThreadPoolStatus() {
        if (pdfThreadPoolExecutor != null) {
            logger.info("PDF线程池状态: 活跃线程={}, 队列大小={}, 完成任务={}, 线程总数={}",
                    pdfThreadPoolExecutor.getActiveCount(),
                    pdfThreadPoolExecutor.getQueue().size(),
                    pdfThreadPoolExecutor.getCompletedTaskCount(),
                    pdfThreadPoolExecutor.getPoolSize());
        }
    }
}