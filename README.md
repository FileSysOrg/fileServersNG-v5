# fileServersNG
The replacement Alfresco file servers subsystem based on the JFileServer file server code. 

In the POM.xml file set the <alfresco.platform.version> to the version of Alfresco that you will be
deploying the fileServersNG file server subsystem on.

One way to find the Alfresco version is by logging in with Share, click on the Alfresco logo at
the bottom of the web page, the popup window should show the Alfresco version, for example '5.2.f'.

To build the fileServersNG AMP use `mvn clean package`, this will create the fileServersNG-v5-1.0.0.amp
file in the target/ folder.

For more complete instructions on how to build and deploy the fileServersNG subsystem see
[here](http://www.filesys.org/wiki/index.php/How_to_build_and_deploy_the_fileServersNG_subsystem).

A Docker image is available containing an Alfresco 5.2 installation with the fileServersNG file server
replacement subsystem deployed, see [here](http://www.filesys.org/wiki/index.php/Using_the_fileServersNG_Docker_Image)
for details of how to download and configure the Docker image. 