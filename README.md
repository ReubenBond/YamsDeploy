# YamsDeploy
Simple command line tool for deploying applications to a [Yams](https://github.com/Microsoft/Yams) cluster.

[![Join the chat at https://gitter.im/Microsoft/Yams](https://badges.gitter.im/Microsoft/Yams.svg)](https://gitter.im/Microsoft/Yams?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

## Features
* Auto-increment version numbers
* Uploads only changed files: unchanged files are copied from previous application versions and other deployed applications.
* Updates Yams' `DeploymentConfig.json`.
* Only deploys applications which have changed.

## Usage
```
Usage - Deploy -options

GlobalOption             Description
ClusterId* (-Cl)         The ClusterId of the Yams cluster.
ConnectionString* (-Co)  The Azure Storage connection string
Version (-V)             The version to upload.
Id (-I)                  The application identifier.
SourceDirectory (-S)     The local directory containing the application.
StdIn (-St)              Whether or not to read application mapping from console input. If true, AppName, SourceDirectory, & Version are ignored. [Default='False']
```

### Example: Deploying a single application:
```
> Deploy.exe `
  -Id orleans `
  -SourceDirectory c:\dev\app\orleans\bin\Debug\ `
  -ClusterId 6229b2c6e47dd882dc74f1952079e421 `
  -ConnectionString DefaultEndpointsProtocol=https;AccountName=shiratake;AccountKey=hunter2
```

### Example: Deploying multiple applications:
Define the mapping between ApplicationId and source directory in a file, eg `applications.json`:
```json
[
    { "Id":"web", "SourceDirectory": "c:\\app\\web\\bin\\Debug"},
    { "Id":"orleans", "SourceDirectory": "c:\\app\\orleans\\bin\\Debug"}
]
```
Pipe that file to YamsDeploy:
```
> cat applications.json | Deploy.exe `
    -ClusterId 6229b2c6e47dd882dc74f1952079e421 `
    -ConnectionString DefaultEndpointsProtocol=https;AccountName=shiratake;AccountKey=hunter2
```