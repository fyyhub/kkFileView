# kkFileView master 自动部署

当前线上 Windows 服务器的实际部署信息如下：

- 部署根目录：`C:\kkFileView-5.0`
- 运行 jar：`C:\kkFileView-5.0\bin\kkFileView-5.0.jar`
- 启动脚本：`C:\kkFileView-5.0\bin\startup.bat`
- 运行配置：`C:\kkFileView-5.0\config\test.properties`
- 健康检查地址：`http://127.0.0.1:8012/`

服务器当前没有安装 `git` 和 `mvn`，因此自动部署链路采用：

1. GitHub Actions 在 `master` 合并后构建 `kkFileView-*.jar`
2. 通过 WinRM 连接 Windows 服务器
3. 由服务器从当前 workflow run 下载 jar artifact
4. 备份线上 jar，替换为新版本
5. 使用现有 `startup.bat` 重启，并做健康检查
6. 如果健康检查失败，则自动回滚旧 jar 并重新拉起

## 需要配置的 GitHub Secrets

- `KK_DEPLOY_HOST`
- `KK_DEPLOY_ARTIFACT_TOKEN`
- `KK_DEPLOY_USERNAME`
- `KK_DEPLOY_PASSWORD`

下面这些可以不配，未配置时会使用默认值：

- `KK_DEPLOY_PORT=5985`
- `KK_DEPLOY_ROOT=C:\kkFileView-5.0`
- `KK_DEPLOY_HEALTH_URL=http://127.0.0.1:8012/`

其中 `KK_DEPLOY_ARTIFACT_TOKEN` 建议使用单独的细粒度 token，只授予当前仓库所需的最小读取权限，不要复用默认 `GITHUB_TOKEN` 到生产服务器。

## Workflow

新增 workflow：`.github/workflows/master-auto-deploy.yml`

- 触发条件：`push` 到 `master`，或手动 `workflow_dispatch`
- 构建产物：`kkfileview-server-jar`
- 部署方式：WinRM + GitHub Actions artifact 下载
