@echo off

REM Checkout the master branch
git checkout master

REM Merge dev-main into master
git merge dev-main

REM Push changes back to the remote
git push origin master
