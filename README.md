# Figlotech

This repository contains all of my handcrafted libraries and tools.
These projects are in the .NET Core csproj version, but they're using NET462
Some things in these projects use stuff that doesnt work out of the box on NETCOREAPP1.1
but these projects might work on linux through Mono (I think)

This code is not "good" for commercial use,
I cannot guarantee that this stuff will work for anyone else's needs.
Or that it will at all work.
It's here just so that maybe people may bash my bad coding and maybe I learn something
Or maybe people like something and suggest or find something interesting and use the ideas on new projects.
And also, specially, it's here as a backup.

# Figlotech.Autokryptex
Provides simple encryption algorithms for quick encrypting stuff. 
Needs from NET462: 
- System.Cryptography for Figlotech.Autokryptex.EncriptionMethods.AesEnkryptor

# Figlotech.BDados
Provides a structure for using rich decoupled data-capable business objects with some level of dependency injection
Still needs a lot of work though, this project implements some DataAccessors, but the main RDBMS DataAccessors are implemented separetely.
Needs from NET462: 
- System.Management for FiglotechTools.BDados.FTH (used to get CPU_ID, hope to remove that need soon)
- System.Data.DataSetExtensions (used for JoinGeneration and ObjectBuilding logic. IRDBMSDataAccessors in this lib use Ado.Net, IDK what's the Ado.Net substitute in NET CORE)

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
