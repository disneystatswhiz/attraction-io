@echo off

:: Run AK07 (WDW)
set ENTITY=ak07
set PARK=ak
set PROPERTY=wdw
set TYPE=standby
start "" /b cmd /c "julia src/main.jl %ENTITY% %PARK% %PROPERTY% %TYPE%"
timeout /t 60 >nul

:: Run HS103 (WDW)
set ENTITY=hs103
set PARK=hs
set PROPERTY=wdw
set TYPE=standby
start "" /b cmd /c "julia src/main.jl %ENTITY% %PARK% %PROPERTY% %TYPE%"
timeout /t 60 >nul

:: Run MK01 (WDW)
set ENTITY=mk01
set PARK=mk
set PROPERTY=wdw
set TYPE=standby
start "" /b cmd /c "julia src/main.jl %ENTITY% %PARK% %PROPERTY% %TYPE%"
timeout /t 60 >nul

:: Run DL01 (DLR)
set ENTITY=dl01
set PARK=dl
set PROPERTY=dlr
set TYPE=standby
start "" /b cmd /c "julia src/main.jl %ENTITY% %PARK% %PROPERTY% %TYPE%"

:: Run AK85 (WDW)
set ENTITY=ak85
set PARK=ak
set PROPERTY=wdw
set TYPE=standby
start "" /b cmd /c "julia src/main.jl %ENTITY% %PARK% %PROPERTY% %TYPE%"

:: Run AK86 (WDW)
set ENTITY=ak86
set PARK=ak
set PROPERTY=wdw
set TYPE=standby
start "" /b cmd /c "julia src/main.jl %ENTITY% %PARK% %PROPERTY% %TYPE%"

:: Run CA10 (DLR)
set ENTITY=ca10
set PARK=ca
set PROPERTY=dlr
set TYPE=priority
start "" /b cmd /c "julia src/main.jl %ENTITY% %PARK% %PROPERTY% %TYPE%"

:: Run AK06 (WDW)
set ENTITY=ak06
set PARK=ak
set PROPERTY=wdw
set TYPE=priority
start "" /b cmd /c "julia src/main.jl %ENTITY% %PARK% %PROPERTY% %TYPE%"

