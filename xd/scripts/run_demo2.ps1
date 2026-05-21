param(
    [switch]$WithDocker,
    [switch]$WithSender,
    [string]$SenderBin = "sender_1min"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Test-WriteDelete {
    param([Parameter(Mandatory = $true)][string]$Dir)
    $test = Join-Path $Dir "__perm_test.tmp"
    try {
        if (-not (Test-Path $Dir)) {
            New-Item -ItemType Directory -Path $Dir -Force | Out-Null
        }
        Set-Content -Path $test -Value "ok" -Encoding ASCII
        Remove-Item -Path $test -Force -ErrorAction Stop
        return $true
    }
    catch {
        Write-Host "权限检查失败: $Dir" -ForegroundColor Red
        Write-Host "错误: $($_.Exception.Message)" -ForegroundColor Red
        return $false
    }
}

function Start-WorkerWindow {
    param(
        [Parameter(Mandatory = $true)][string]$Title,
        [Parameter(Mandatory = $true)][string]$WorkingDir,
        [Parameter(Mandatory = $true)][string]$Command,
        [Parameter(Mandatory = $true)][string]$TargetDir
    )

    $cmd = @"
`$Host.UI.RawUI.WindowTitle = '$Title'
Set-Location '$WorkingDir'
`$env:CARGO_TARGET_DIR = '$TargetDir'
`$env:CARGO_INCREMENTAL = '0'
$Command
"@
    Start-Process powershell -ArgumentList @(
        "-NoExit",
        "-ExecutionPolicy", "Bypass",
        "-Command", $cmd
    ) | Out-Null
}

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Split-Path -Parent $scriptDir
$senderRepo = Join-Path $repoRoot "tcp_frame_sender"
$collectorTarget = Join-Path $repoRoot "run_target_collector"
$uiTarget = Join-Path $repoRoot "run_target_ui"
$senderTarget = Join-Path $repoRoot "run_target_sender"

Write-Host "== demo2 一键启动 ==" -ForegroundColor Cyan
Write-Host "repo: $repoRoot"

$cfgPath = Join-Path $repoRoot "config.toml"
$cfgExample = Join-Path $repoRoot "config.toml.example"
if (-not (Test-Path $cfgPath) -and (Test-Path $cfgExample)) {
    Copy-Item $cfgExample $cfgPath
    Write-Host "已自动创建 config.toml（来自 config.toml.example）" -ForegroundColor Yellow
}

if ($WithDocker) {
    Write-Host "启动 PostgreSQL 容器..." -ForegroundColor Yellow
    Push-Location $repoRoot
    try {
        docker compose up -d postgres | Out-Host
    }
    finally {
        Pop-Location
    }
}

if (-not (Test-WriteDelete -Dir $collectorTarget)) {
    Write-Host ""
    Write-Host "请先处理目录权限后再运行脚本，建议：" -ForegroundColor Yellow
    Write-Host "1) 以管理员身份打开 PowerShell"
    Write-Host "2) 执行: icacls `"$repoRoot`" /grant `"$env:USERDOMAIN\$env:USERNAME:(OI)(CI)F`" /T"
    Write-Host "3) 或将仓库移动到非受控目录（如 D:\\dev\\demo2）"
    exit 1
}
if (-not (Test-WriteDelete -Dir $uiTarget)) {
    Write-Host "UI target 目录不可写不可删，请按上面的权限步骤修复。" -ForegroundColor Yellow
    exit 1
}
if ($WithSender) {
    if (-not (Test-WriteDelete -Dir $senderTarget)) {
        Write-Host "Sender target 目录不可写不可删，请按上面的权限步骤修复。" -ForegroundColor Yellow
        exit 1
    }
}

Write-Host "启动 collector_service 窗口..." -ForegroundColor Green
Start-WorkerWindow `
    -Title "demo2-collector_service" `
    -WorkingDir $repoRoot `
    -TargetDir $collectorTarget `
    -Command "cargo run --bin collector_service -j 1"

Start-Sleep -Seconds 2

Write-Host "启动 ui_client 窗口..." -ForegroundColor Green
Start-WorkerWindow `
    -Title "demo2-ui_client" `
    -WorkingDir $repoRoot `
    -TargetDir $uiTarget `
    -Command "cargo run --bin ui_client -j 1"

if ($WithSender) {
    if (Test-Path $senderRepo) {
        Start-Sleep -Seconds 1
        Write-Host "启动 sender 窗口: $SenderBin" -ForegroundColor Green
        Start-WorkerWindow `
            -Title "demo2-$SenderBin" `
            -WorkingDir $senderRepo `
            -TargetDir $senderTarget `
            -Command "cargo run --bin $SenderBin -j 1"
    }
    else {
        Write-Warning "未找到 sender 工程目录: $senderRepo，已跳过 sender 启动。"
    }
}

Write-Host ""
Write-Host "已发起启动。" -ForegroundColor Cyan
Write-Host "健康检查: http://127.0.0.1:19012/health"
Write-Host "就绪检查: http://127.0.0.1:19012/ready"
Write-Host ""
Write-Host "示例:"
Write-Host "  .\scripts\run_demo2.ps1"
Write-Host "  .\scripts\run_demo2.ps1 -WithDocker"
Write-Host "  .\scripts\run_demo2.ps1 -WithDocker -WithSender"
