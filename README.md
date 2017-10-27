# Figlotech

This repository contains my "top notch" set of personal gimmicks.
This code is not good, and I'm aware.
This contains a lot of stuff that is better implemented elsewhere, like Tasking.
This code is not documented and written with the purpose of learning and exploring different ways to write less to achieve more.

# Figlotech.Core

Exposes basic interfacing concepts that can be used across several types of applications and which implementations can be easily replaced. I really believe in standardized contracts that could be used anywhere, these contracts should make classes that use them more resistant to refactoring, through the abstraction of implementations, for example: IFileSystem here represents "some thing that provides files and folders", it doesn't matter if its using System.IO, Blob Storage, In-Memory documents or other, the class using an IFileSystem only cares that the injected implementation provide files and folders. This makes it easier for me to switch gigantic processes from using local filesystem to cloud or vice-versa with two or so lines of code.
In this assembly we have:
 - The Fi Class, its an empty "utility" singleton, its whole static functionality rely on extension classes/methods.
 - Object Extensions: 
     - Provides easy To/From json to any object, powered by Newtonsoft.Json
     - Provides easy To/From Dictionary<string, object>
     - Provides easy access to ObjectReflector instance for treating runtime object as Dictionary without actually turning it into a Dictionary or using dynamic
     - Provides easy CopyFrom class to copy values from another object (shallow memberwise copy.
 - AutoKryptex: Basic 'cryptography', its really way too basic.
 - FileAccessAbstractions
     - Defines a model of IFileSystem that represents a repository able to provide files in folders.
     - Contains SmartCopy class for mirroring folders (like Robocopy), its a bit clumsy still, but I already use it for update systems and it supports BlobFileAccessor, and GZip so uploading to azure is just easy.
 - Helpers
     - Tools for a step higher/easier reflecting through objects and types.
 - Basic chronometer
 - RID generation
 - Basic other stuff.
 - Crazy experimental stuff.

# Figlotech.BDados

It works almost like Dapper, but in my way. Its my personal way of wrapping the Ado .Net and its been working nicely since 2013. In spite of the ongoing breaking changes and refactorings.

# imgdput
This command line tool compares images in a folder and separates duplicate images from uniques
Usage: imgdupt <directory_path> <options>
Options:
  -sz <size:int> :
    Sets the precision, higher value means more accurate but slower comparison
  -mindiff <mindiff:int> :
    Sets the minimum difference between images (in %) for the comparator to consider them different
  -tol <tolerance:int> :
    Sets the pixel tolerance, low values may cause the comparator to see 2 apparently equal images as different because of differing artifact/quality

The comparator is able to read any GDI+ compliant format and even compare images of different extensions, it will always give preference for larger images when finding duplicates.
The comparator can be easily adapted to netcoreapp (and I might do it soon), it only uses System.Drawing and System.Drawing.Imaging that are absent there, but there is a very good 3rd party replacement on NuGet.
