$tools = "go", "docker", "kubectl", "kind", "helm"
foreach ($tool in $tools) {
    $command = Get-Command $tool -ErrorAction SilentlyContinue
    if ($null -eq $command) {
        Write-Output "$tool`: not found"
    } else {
        Write-Output "$tool`: $($command.Source)"
    }
}
