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
Provides simple encryption algorithms for quick encrypting.
There's something new and special about this lib, it generates keys using an unusual pseudo-random algorithm.
Also, the generation algorithm allows the definition of a global "AppKey" (through calling static `CrossRandom.UseAppKey(String key)`)
AND an instance key.
You can call the UseInstanceKey multiple times and all of the keys will affect the final results of the overall generator.
So if you set multiple strings as keys, you'll need to know all of them in the same order to achieve the same ordered results as the first time.
Now, a disclaimer: I'm really bad at math and I don't really know if this is actually useful or way too rubbish. I mean, AES and RSA involve all of that crazy mathmagic I can't legitemately keep with. this is just: Take these primes and manipulate them arround;
Objective: A simple algorithm able to encrypt stuff and keep this encrypted data safe even if everyone and their mom's know the code, requiring reverse engineering in the binary in order to find the real secret to decrypt it.
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
