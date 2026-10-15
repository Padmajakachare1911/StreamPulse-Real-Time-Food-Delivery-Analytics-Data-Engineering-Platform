<#
.SYNOPSIS
    Clean up Demo 3 (data quality / duplicates). Removes only the demo's own topic, database and folders.
    See scripts\reset_demo_state.ps1 for exactly what is and is not touched.
#>
. "$PSScriptRoot\reset_demo_state.ps1" -Demo 3 @args
