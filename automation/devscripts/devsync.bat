@echo off

REM Checkout the master branch
git checkout master

REM Pull all changes from origin
git pull origin master

REM Checkout the dev-main branch
git checkout dev-main

REM Pull any other changes for the dev-main branch
git pull origin dev-main

REM Merge master into dev-main
git merge master

REM Push changes back to the remote
git push origin dev-main
