package cn.keking.service;

import cn.keking.config.ConfigConstants;
import cn.keking.model.FileAttribute;
import cn.keking.utils.RemoveSvgAdSimple;
import com.aspose.cad.*;
import com.aspose.cad.fileformats.cad.CadDrawTypeMode;
import com.aspose.cad.fileformats.tiff.enums.TiffExpectedFormat;
import com.aspose.cad.imageoptions.*;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.*;

/**
 * CAD文件转换服务
 * @author yudian-it
 */
@Component
public class CadToPdfService {
    private static final Logger logger = LoggerFactory.getLogger(CadToPdfService.class);

    /**
     * CAD转换线程池
     */
    private ExecutorService pool;

    /**
     * 初始化线程池
     */
    @PostConstruct
    public void init() {
        try {
            int threadCount = getThreadPoolSize();

            // 使用 ThreadPoolExecutor 而不是 FixedThreadPool，便于控制队列和拒绝策略
            int queueCapacity = getQueueCapacity();
            // 核心线程数
            // 最大线程数（与核心线程数相同，实现固定大小）
            // 空闲线程存活时间（秒）
            // 有界队列，避免内存溢出
            // 拒绝策略：由调用线程执行
            /**
             * 线程池监控
             */
            ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(
                    threadCount,      // 核心线程数
                    threadCount,      // 最大线程数（与核心线程数相同，实现固定大小）
                    60L,              // 空闲线程存活时间（秒）
                    TimeUnit.SECONDS,
                    new LinkedBlockingQueue<>(queueCapacity),  // 有界队列，避免内存溢出
                    // 修改线程工厂部分
                    r -> {
                        Thread t = new Thread(r);
                        // 使用时间戳和随机数生成唯一标识
                        String threadId = System.currentTimeMillis() + "-" +
                                ThreadLocalRandom.current().nextInt(1000);
                        t.setName("cad-convert-pool-" + threadId);
                        t.setUncaughtExceptionHandler((thread, throwable) ->
                                logger.error("CAD转换线程未捕获异常: {}", thread.getName(), throwable));
                        return t;
                    },
                    new ThreadPoolExecutor.CallerRunsPolicy()  // 拒绝策略：由调用线程执行
            );

            // 允许核心线程超时回收
            threadPoolExecutor.allowCoreThreadTimeOut(true);

            pool = threadPoolExecutor;

            logger.info("CAD转换线程池初始化完成，线程数: {}, 队列容量: {}",
                    threadCount, queueCapacity);

        } catch (Exception e) {
            logger.error("CAD转换线程池初始化失败", e);
            // 提供默认值
            int defaultThreads = Math.max(1, Runtime.getRuntime().availableProcessors() / 2);
            pool = Executors.newFixedThreadPool(defaultThreads);
            logger.warn("使用默认线程池配置，线程数: {}", defaultThreads);
        }
    }

    /**
     * 获取线程池大小配置
     */
    private int getThreadPoolSize() {
        try {
            int threadCount = ConfigConstants.getCadThread();
            if (threadCount <= 0) {
                threadCount = Runtime.getRuntime().availableProcessors();
                logger.warn("CAD线程数配置无效，使用CPU核心数: {}", threadCount);
            }
            // 限制最大线程数，避免资源耗尽
            int maxThreads = Runtime.getRuntime().availableProcessors() * 2;
            if (threadCount > maxThreads) {
                logger.warn("CAD线程数配置过大({})，限制为: {}", threadCount, maxThreads);
                threadCount = maxThreads;
            }
            return threadCount;
        } catch (Exception e) {
            logger.error("获取CAD线程数配置失败", e);
            return Math.max(1, Runtime.getRuntime().availableProcessors() / 2);
        }
    }

    /**
     * 获取队列容量
     */
    private int getQueueCapacity() {
        // 根据线程数动态计算队列容量
        int threadCount = getThreadPoolSize();
        return threadCount * 10;  // 每个线程10个待处理任务
    }

    /**
     * 优雅关闭线程池
     */
    @PreDestroy
    public void shutdown() {
        if (pool != null && !pool.isShutdown()) {
            gracefulShutdown(pool, getShutdownTimeout());
        }
    }

    /**
     * 获取关闭超时时间
     */
    private long getShutdownTimeout() {
        try {
            return Long.parseLong(ConfigConstants.getCadTimeout());
        } catch (Exception e) {
            logger.warn("获取CAD关闭超时时间失败，使用默认值60秒", e);
            return 60L;
        }
    }

    /**
     * 通用线程池优雅关闭方法
     */
    private void gracefulShutdown(ExecutorService executor, long timeoutSeconds) {
        logger.info("开始关闭{}...", "CAD转换线程池");

        // 停止接收新任务
        executor.shutdown();

        try {
            // 等待现有任务完成
            if (!executor.awaitTermination(timeoutSeconds, TimeUnit.SECONDS)) {
                logger.warn("{}超时未关闭，尝试强制关闭...", "CAD转换线程池");

                // 取消所有未完成的任务
                executor.shutdownNow();

                // 再次等待
                if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                    logger.error("{}无法正常关闭，可能存在挂起的任务", "CAD转换线程池");
                } else {
                    logger.info("{}已强制关闭", "CAD转换线程池");
                }
            } else {
                logger.info("{}已正常关闭", "CAD转换线程池");
            }
        } catch (InterruptedException e) {
            logger.error("{}关闭时被中断", "CAD转换线程池", e);
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * CAD文件转换
     *
     * @param inputFilePath  输入CAD文件路径
     * @param outputFilePath 输出文件路径
     * @param cadPreviewType 预览类型 (svg/pdf/tif)
     * @param fileAttribute  文件属性
     * @return 转换成功返回true，失败返回false
     */
    public boolean cadToPdf(String inputFilePath, String outputFilePath,
                            String cadPreviewType, FileAttribute fileAttribute) {

        final InterruptionTokenSource source = new InterruptionTokenSource();

        try {
            // 验证输入参数
            if (!validateInputParameters(inputFilePath, outputFilePath, cadPreviewType)) {
                return false;
            }

            // 创建输出选项
            final SvgOptions svgOptions = new SvgOptions();
            final PdfOptions pdfOptions = new PdfOptions();
            final TiffOptions tiffOptions = new TiffOptions(TiffExpectedFormat.TiffJpegRgb);

            // 创建输出目录
            createOutputDirectoryIfNeeded(outputFilePath, fileAttribute.isCompressFile());

            File outputFile = new File(outputFilePath);

            // 加载并转换CAD文件
            LoadOptions loadOptions = createLoadOptions();
            try (Image cadImage = Image.load(inputFilePath, loadOptions)) {

                CadRasterizationOptions rasterizationOptions = createRasterizationOptions(cadImage);
                configureOutputOptions(cadPreviewType, rasterizationOptions, source,
                        svgOptions, pdfOptions, tiffOptions);

                Callable<Boolean> conversionTask = createConversionTask(cadPreviewType, outputFile,
                        cadImage, source,
                        svgOptions, pdfOptions, tiffOptions);

                Future<Boolean> result = pool.submit(conversionTask);

                return executeWithTimeout(result, source, cadImage, inputFilePath);
            }
        } catch (Exception e) {
            logger.error("CAD文件转换失败: {}", inputFilePath, e);
            return false;
        } finally {
            // 确保资源释放
            try {
                source.dispose();
            } catch (Exception e) {
                logger.warn("释放CAD中断令牌资源失败", e);
            }

            // SVG文件后处理
            if ("svg".equals(cadPreviewType)) {
                postProcessSvgFile(outputFilePath);
            }
        }
    }

    /**
     * 验证输入参数
     */
    private boolean validateInputParameters(String inputFilePath, String outputFilePath,
                                            String cadPreviewType) {
        if (inputFilePath == null || inputFilePath.trim().isEmpty()) {
            logger.error("输入文件路径为空");
            return false;
        }

        File inputFile = new File(inputFilePath);
        if (!inputFile.exists()) {
            logger.error("输入文件不存在: {}", inputFilePath);
            return false;
        }

        if (outputFilePath == null || outputFilePath.trim().isEmpty()) {
            logger.error("输出文件路径为空");
            return false;
        }

        if (!isSupportedPreviewType(cadPreviewType)) {
            logger.error("不支持的预览类型: {}", cadPreviewType);
            return false;
        }

        return true;
    }

    /**
     * 检查是否支持的预览类型
     */
    private boolean isSupportedPreviewType(String previewType) {
        return "svg".equalsIgnoreCase(previewType) ||
                "pdf".equalsIgnoreCase(previewType) ||
                "tif".equalsIgnoreCase(previewType) ||
                "tiff".equalsIgnoreCase(previewType);
    }

    /**
     * 创建输出目录
     */
    private void createOutputDirectoryIfNeeded(String outputFilePath, boolean isCompressFile) {
        if (!isCompressFile) {
            return;
        }

        File outputFile = new File(outputFilePath);
        File parentDir = outputFile.getParentFile();

        if (parentDir != null && !parentDir.exists()) {
            boolean created = parentDir.mkdirs();
            if (created) {
                logger.debug("创建输出目录: {}", parentDir.getAbsolutePath());
            } else {
                logger.warn("无法创建输出目录: {}", parentDir.getAbsolutePath());
            }
        }
    }

    /**
     * 创建加载选项
     */
    private LoadOptions createLoadOptions() {
        LoadOptions opts = new LoadOptions();
        opts.setSpecifiedEncoding(CodePages.SimpChinese);
        return opts;
    }

    /**
     * 创建光栅化选项
     */
    private CadRasterizationOptions createRasterizationOptions(Image cadImage) {
        RasterizationQuality quality = new RasterizationQuality();
        RasterizationQualityValue highQuality = RasterizationQualityValue.High;

        quality.setArc(highQuality);
        quality.setHatch(highQuality);
        quality.setText(highQuality);
        quality.setOle(highQuality);
        quality.setObjectsPrecision(highQuality);
        quality.setTextThicknessNormalization(true);

        CadRasterizationOptions options = new CadRasterizationOptions();
        options.setBackgroundColor(Color.getWhite());
        options.setPageWidth(cadImage.getWidth());
        options.setPageHeight(cadImage.getHeight());
        options.setUnitType(cadImage.getUnitType());
        options.setAutomaticLayoutsScaling(false);
        options.setNoScaling(false);
        options.setQuality(quality);
        options.setDrawType(CadDrawTypeMode.UseObjectColor);
        options.setExportAllLayoutContent(true);
        options.setVisibilityMode(VisibilityMode.AsScreen);

        return options;
    }

    /**
     * 配置输出选项
     */
    private void configureOutputOptions(String previewType,
                                        CadRasterizationOptions rasterizationOptions,
                                        InterruptionTokenSource source,
                                        SvgOptions svgOptions,
                                        PdfOptions pdfOptions,
                                        TiffOptions tiffOptions) {

        String type = previewType.toLowerCase();
        switch (type) {
            case "svg":
                svgOptions.setVectorRasterizationOptions(rasterizationOptions);
                svgOptions.setInterruptionToken(source.getToken());
                break;
            case "pdf":
                pdfOptions.setVectorRasterizationOptions(rasterizationOptions);
                pdfOptions.setInterruptionToken(source.getToken());
                break;
            case "tif":
            case "tiff":
                tiffOptions.setVectorRasterizationOptions(rasterizationOptions);
                tiffOptions.setInterruptionToken(source.getToken());
                break;
            default:
                throw new IllegalArgumentException("不支持的预览类型: " + previewType);
        }
    }

    /**
     * 创建转换任务
     */
    private Callable<Boolean> createConversionTask(String previewType,
                                                   File outputFile,
                                                   Image cadImage,
                                                   InterruptionTokenSource source,
                                                   SvgOptions svgOptions,
                                                   PdfOptions pdfOptions,
                                                   TiffOptions tiffOptions) {

        return () -> {
            try (OutputStream outputStream = new FileOutputStream(outputFile)) {
                String type = previewType.toLowerCase();

                switch (type) {
                    case "svg":
                        cadImage.save(outputStream, svgOptions);
                        break;
                    case "pdf":
                        cadImage.save(outputStream, pdfOptions);
                        break;
                    case "tif":
                    case "tiff":
                        cadImage.save(outputStream, tiffOptions);
                        break;
                    default:
                        throw new IllegalStateException("不支持的预览类型: " + previewType);
                }

                logger.debug("CAD文件转换成功: {} -> {}", cadImage, outputFile.getPath());
                return true;

            } catch (IOException e) {
                logger.error("保存转换结果失败: {}", outputFile.getPath(), e);
                throw e;
            } catch (Exception e) {
                logger.error("CAD转换过程异常", e);
                throw e;
            }
        };
    }

    /**
     * 执行带超时的转换
     */
    private boolean executeWithTimeout(Future<Boolean> result,
                                       InterruptionTokenSource source,
                                       Image cadImage,
                                       String inputFilePath) {
        long timeout = getConversionTimeout();

        try {
            Boolean success = result.get(timeout, TimeUnit.SECONDS);
            return Boolean.TRUE.equals(success);

        } catch (TimeoutException e) {
            logger.error("CAD转换超时，文件: {}，超时时间: {}秒", inputFilePath, timeout, e);
            handleTimeout(result, source);
            return false;

        } catch (InterruptedException e) {
            logger.error("CAD转换被中断，文件: {}", inputFilePath, e);
            Thread.currentThread().interrupt();
            handleInterruption(result);
            return false;

        } catch (ExecutionException e) {
            logger.error("CAD转换执行异常，文件: {}", inputFilePath, e);
            return false;

        } catch (Exception e) {
            logger.error("CAD转换未知异常，文件: {}", inputFilePath, e);
            return false;
        }
    }

    /**
     * 获取转换超时时间
     */
    private long getConversionTimeout() {
        try {
            long timeout = Long.parseLong(ConfigConstants.getCadTimeout());
            if (timeout <= 0) {
                timeout = 300L; // 默认5分钟
                logger.warn("CAD转换超时时间配置无效，使用默认值: {}秒", timeout);
            }
            return timeout;
        } catch (NumberFormatException e) {
            logger.warn("解析CAD转换超时时间失败，使用默认值300秒", e);
            return 300L;
        }
    }

    /**
     * 处理超时情况
     */
    private void handleTimeout(Future<Boolean> result, InterruptionTokenSource source) {
        try {
            source.interrupt();
        } catch (Exception e) {
            logger.warn("中断CAD转换过程失败", e);
        }

        try {
            boolean cancelled = result.cancel(true);
            logger.debug("超时任务取消结果: {}", cancelled ? "成功" : "失败");
        } catch (Exception e) {
            logger.warn("取消超时任务失败", e);
        }
    }

    /**
     * 处理中断情况
     */
    private void handleInterruption(Future<Boolean> result) {
        try {
            result.cancel(true);
        } catch (Exception e) {
            logger.warn("取消被中断的任务失败", e);
        }
    }

    /**
     * SVG文件后处理
     */
    private void postProcessSvgFile(String outputFilePath) {
        try {
            RemoveSvgAdSimple.removeSvgAdFromFile(outputFilePath);
            logger.debug("SVG文件后处理完成: {}", outputFilePath);
        } catch (Exception e) {
            logger.warn("SVG文件后处理失败: {}", outputFilePath, e);
        }
    }
}