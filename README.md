# Integration Sample Code In VB.net

This code sample explains how to integrate with Nana Servers using VB.net.

In this code sample the following steps [commands] are executed

- Connection to MySql Database
- Execution of Command to MySql Database
- Serializing result into `JSON` (JavaScript Oriented Notation) and divide total result into batch of 1000
- Send the batches to Nana Servers
- Log out result for each batch

## Prerequisites
- Install `dotnet` framework from this link [Microsoft DotNet](https://dotnet.microsoft.com/download/dotnet-framework)
- Set the database connection string, adapter and query

## Download
To use this demo clone this project using either `git` CLI (command line interface) or the download button in the top-right side
of the page
*Using git cli*
```
git clone https://github.com/abdul-elah-js/vbdotnet-example.git
```
## Third Party Packages
Install third party packages as follow:
```
dotnet add package Microsoft.Net.Http
```
*Adapter shall be replaced with the database used in your system*
```
dotnet add package MySql.Data
```
```
dotnet add package Newtonsoft.Json
```
```
dotnet add package System.Data.DataSetExtensions
```
## Run the Example
To run the example simply navigate to the project root and type:
```
dotnet run
```
