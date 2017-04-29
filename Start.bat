@echo off


if "%NETCF_PATH%" == "" (
  set NETCF_PATH=C:\Windows\Microsoft.NET\Framework\v4.0.30319)

if DEFINED REF ( set REF= )

set REF=%REF% "/r:%NETCF_PATH%\MsCorlib.dll"
set REF=%REF% "/r:%NETCF_PATH%\System.Data.dll"
set REF=%REF% "/r:System.Data.SQLite.dll"
set REF=%REF% "/r:%NETCF_PATH%\System.dll"
set REF=%REF% "/r:%NETCF_PATH%\System.Xml.dll"
set REF=%REF% "/r:%NETCF_PATH%\System.Core.dll"
set REF=%REF% "/r:%NETCF_PATH%\System.Linq.dll"
set REF=%REF% "/r:%NETCF_PATH%\System.Globalization.dll"
set REF=%REF% "/r:%NETCF_PATH%\Microsoft.VisualBasic.dll"

"%NETCF_PATH%\csc" -noconfig -nologo %REF% %* /platform:x86 Program.cs

chcp 1251
echo Компиляция успешно завершена, можно закрыть окно
echo Compilation completed successfully, you can close this window

:start

goto start









