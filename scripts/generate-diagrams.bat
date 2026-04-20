@echo off
REM Script to generate class diagrams from Go code
REM Run from project root: scripts\generate-diagrams.bat

echo.
echo === GoPlantUML Class Diagram Generator ===
echo.

REM Check if goplantuml is installed
where goplantuml >nul 2>nul
if errorlevel 1 (
    echo Installing GoPlantUML...
    call go install github.com/jfeliu007/goplantuml/cmd/goplantuml@latest
)

REM Create output directory
if not exist "docs\diagrams" mkdir docs\diagrams

echo Generating class diagrams...
echo.

REM Transaction/Isolation system
echo ^✓ Generating transaction package diagram...
goplantuml -output docs/diagrams/txn_classes.puml ./internal/txn
if errorlevel 1 goto error

echo ^✓ Generating isolation strategies diagram...
goplantuml -output docs/diagrams/isolation_classes.puml ./internal/isolation
if errorlevel 1 goto error

REM Storage system
echo ^✓ Generating storage package diagram...
goplantuml -output docs/diagrams/storage_classes.puml ./internal/storage
if errorlevel 1 goto error

echo ^✓ Generating B+ Tree implementation diagram...
goplantuml -output docs/diagrams/btree_classes.puml ./internal/storage/btree
if errorlevel 1 goto error

echo ^✓ Generating LSM Tree implementation diagram...
goplantuml -output docs/diagrams/lsm_classes.puml ./internal/storage/lsm
if errorlevel 1 goto error

echo ^✓ Generating buffer pool diagram...
goplantuml -output docs/diagrams/buffer_classes.puml ./internal/storage/buffer
if errorlevel 1 goto error

REM Concurrency
echo ^✓ Generating concurrency control diagram...
goplantuml -output docs/diagrams/concurrency_classes.puml ./internal/concurrency
if errorlevel 1 goto error

REM Full architecture
echo ^✓ Generating full architecture diagram...
goplantuml -output docs/diagrams/full_architecture.puml ./internal
if errorlevel 1 goto error

echo.
echo === Success! ===
echo Generated diagrams:
for %%f in (docs\diagrams\*.puml) do echo   - %%~nxf
echo.
echo View diagrams:
echo   - VS Code: Install 'PlantUML' extension
echo   - Online: http://www.plantuml.com/plantuml/uml/
echo   - Docs: Check docs/architecture/diagrams.md
goto end

:error
echo ERROR: Failed to generate diagrams
exit /b 1

:end
