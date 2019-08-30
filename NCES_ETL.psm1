
Function Get-RLocation {
    If (Test-Path 'C:\Program Files\R') {
        $result = Get-ChildItem 'C:\Program Files\R' | sort LastWriteTime -Descending | Select -First 1
        if($result) {
            $Script:RLocation = $result.FullName
        }
    }
    return $Script:RLocation
}

Function Invoke-NCESETL {

    $rbinpath = Join-Path $(Get-RLocation) "bin"

    if($Env:PATH.IndexOf($path) -Eq -1) {
        $Env:PATH = "$Env:PATH;$rbinpath"
    }
    $Env:HADOOP_HOME = "$Env:HOME\spark-2.4.3-bin-hadoop2.7"
    $Env:SPARK_HOME = "$Env:HOME\spark-2.4.3-bin-hadoop2.7"

    . "$Env:SPARK_HOME\bin\spark-submit.cmd" --master local[*] load_NCES.R
}

Function Get-NCESData {

    $rbinpath = Join-Path $(Get-RLocation) "bin"

    . "$rbinpath\Rscript.exe" download_NCES_data.R
}

Function Import-NCESData {
    # TODO: this only works if you've imported RMP module; should replace these fn calls with calls to bcp
    $files = Get-ChildItem output -Filter *.csv
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
