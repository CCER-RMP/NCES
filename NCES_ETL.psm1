
Function Get-RLocation {
    If (Test-Path 'C:\Program Files\R') {
        $result = Get-ChildItem 'C:\Program Files\R' | sort LastWriteTime -Descending | Select -First 1
        if($result) {
            $Script:RLocation = $result.FullName
        }
    }
    return $Script:RLocation
}

Function Install-NCESRLibraries {
    $rbinpath = Join-Path $(Get-RLocation) "bin"

    . "$rbinpath\Rscript.exe" -e "packrat::restore()"
}

Function Get-NCESClusterURL {
    $ip = $(Get-NetIPAddress -AddressFamily IPv4)[0].IPAddress

    return "spark://$($ip):7077"
}

Function Invoke-NCESETL {
    # run in local mode unless -Cluster switch is specified
    Param(
        [Switch] $Cluster = $false
    )

    $rbinpath = Join-Path $(Get-RLocation) "bin"

    if($Env:PATH.IndexOf($path) -Eq -1) {
        $Env:PATH = "$Env:PATH;$rbinpath"
    }
    $Env:HADOOP_HOME = "$Env:HOME\spark-2.4.3-bin-hadoop2.7"
    $Env:SPARK_HOME = "$Env:HOME\spark-2.4.3-bin-hadoop2.7"

    $start = $(Get-Date)

    $args = @()

    if($Cluster) {
        $args = $args + @("--master", $(Get-NCESClusterURL))
    } else {
        $args = $args + @("--master", "local[*]")
        # in local mode, setting spark.executor.memory doesn't work, it's driver memory
        # that controls executor's storage memory. we need this to avoid outofmemory errors
        # when calling cache()
        $args = $args + @("--driver-memory", "3g")
    }

    . "$Env:SPARK_HOME\bin\spark-submit.cmd" $args load_NCES.R

    $end = $(Get-Date)
    $elapsedTime = $end - $start
    $totalTime = "{0:HH:mm:ss}" -f ([datetime]$elapsedTime.Ticks)
    Write-Output "Took $totalTime"
}

Function Get-NCESData {

    $rbinpath = Join-Path $(Get-RLocation) "bin"

    . "$rbinpath\Rscript.exe" download_NCES_data.R
}

Function Import-NCESData {
    # TODO: this only works if you've imported RMP module; should replace these fn calls with calls to bcp
    $files = Get-ChildItem output\NCESSchools -Filter *.csv
    $first = $true
    $files | ForEach-Object {
        $file =  $_.FullName
        if($first) {
           Import-RmpTable $file dbo.NCES_test -Database S275 -CreateTable -FileType TAB
        } else {
            Import-RmpTable $file dbo.NCES_test -Database S275 -FileType TAB
        }
        $first = $false
    }
}

Function Start-NCESMaster {
    # experimenting with standalone cluster mode
    $rbinpath = Join-Path $(Get-RLocation) "bin"

    if($Env:PATH.IndexOf($path) -Eq -1) {
        $Env:PATH = "$Env:PATH;$rbinpath"
    }
    $Env:HADOOP_HOME = "$Env:HOME\spark-2.4.3-bin-hadoop2.7"
    $Env:SPARK_HOME = "$Env:HOME\spark-2.4.3-bin-hadoop2.7"

   . "$Env:SPARK_HOME\bin\spark-class" org.apache.spark.deploy.master.Master
}

Function Start-NCESWorker {
    # experimenting with standalone cluster mode
    $rbinpath = Join-Path $(Get-RLocation) "bin"

    if($Env:PATH.IndexOf($path) -Eq -1) {
        $Env:PATH = "$Env:PATH;$rbinpath"
    }
    $Env:HADOOP_HOME = "$Env:HOME\spark-2.4.3-bin-hadoop2.7"
    $Env:SPARK_HOME = "$Env:HOME\spark-2.4.3-bin-hadoop2.7"

   . "$Env:SPARK_HOME\bin\spark-class" org.apache.spark.deploy.worker.Worker $(Get-NCESClusterURL) --cores 1 --memory 1G
}

Function Start-NCESCluster {
    Param(
        [Int] $Workers = -1
    )

    if($Workers -Eq -1) {
        Get-WmiObject -class Win32_processor | ft systemname,Name,DeviceID,NumberOfCores,NumberOfLogicalProcessors, Addresswidth
        $Workers = $(Get-WmiObject -class Win32_processor).NumberOfLogicalProcessors
    }

    Start-NCESMaster
    Start-Sleep 5
    For ($i=0; $i -lt $Workers; $i++) {
        Start-NCESWorker
        Start-Sleep 2
    }
}
