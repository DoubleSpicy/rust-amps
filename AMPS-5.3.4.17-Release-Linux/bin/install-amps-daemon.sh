#!/bin/sh
############################################################################
##
## Copyright (c) 2012-2024 60East Technologies Inc., All Rights Reserved.
##
## This computer software is owned by 60East Technologies Inc. and is
## protected by U.S. copyright laws and other laws and by international
## treaties.  This computer software is furnished by 60East Technologies 
## Inc. pursuant to a written license agreement and may be used, copied,
## transmitted, and stored only in accordance with the terms of such
## license agreement and with the inclusion of the above copyright notice.
## This computer software or any other copies thereof may not be provided
## or otherwise made available to any other person.
##
## U.S. Government Restricted Rights.  This computer software: (a) was
## developed at private expense and is in all respects the proprietary
## information of 60East Technologies Inc; (b) was not developed with
## government funds; (c) is a trade secret of 60East Technologies Inc.
## for all purposes of the Freedom of Information Act; and (d) is a
## commercial item and thus, pursuant to Section 12.212 of the Federal
## Acquisition Regulations (FAR) and DFAR Supplement Section 227.7202,
## Government's use, duplication or disclosure of the computer software
## is subject to the restrictions set forth by 60East Technologies Inc.
##
############################################################################

# Purpose: This script installs the 60East Technologies AMPS daemon.

INSTALLPATH=/opt/amps
RUNPATH=/var/run/amps
CONFIGPATH=/opt/etc/amps


echo "Testing system install style"

## Capability check for installing things as daemons
##
## Rather than trying to figure out all of the permutations on which systems
## have particular ways of starting things, test for the ways this script
## knows how to set things up.
##

STYLE="Unknown"

# Ensure we have sbin
PATH=/sbin:$PATH

exec 3>&2
exec 2>/dev/null

# Prefer systemd when available.

if [ -d /usr/lib/systemd ] && [ ! -z `which systemctl` ] && [ "$STYLE" = 'Unknown' ]; then
   STYLE="systemd"
   echo "Using systemd service definitions."
   # Now find a good installpath
   if [ -d /usr/lib/systemd/system ]; then
       SYSTEMD_PATH=/usr/lib/systemd/system
   else
       SYSTEMD_PATH=/usr/lib/systemd/
   fi
fi

if [ ! -z `which update-rc.d` ] && [ "$STYLE" = 'Unknown' ]; then
   STYLE="update-rc"
   echo "Using Debian-style daemon installation."
fi

# Prefer systemd over chkconfig where available.

if [ ! -z `which chkconfig` ] && [ -e /etc/init.d/functions ] && [ "$STYLE" = 'Unknown' ]; then
   STYLE="chkconfig" 
   echo "Using RedHat-style chkconfig and SYSV-style scripts."
fi
exec 2>&3

## Those are the styles we know how to detect. Provide output.

if [ "$STYLE" = "Unknown" ]; then
  echo "$0 does not have an installation target for this system. Please provide details of your system and version to 60East support."
  exit 1;
fi


mkdir -p $INSTALLPATH
mkdir -p $RUNPATH
mkdir -p $CONFIGPATH

# Copy the bin and lib directories to the run directory.
cp -r ../bin $INSTALLPATH/.
cp -r ../lib $INSTALLPATH/.


echo "Installed AMPS binaries to $INSTALLPATH"


case $STYLE in
     update*)
       echo "Copying init script to /etc/init.d/amps"
       cp -f amps-init-script-debian /etc/init.d/amps
       ERROR=$?
       if [ ! -x /etc/init.d/amps ]; then
          echo "Installation failed: $ERROR."
          exit $ERROR
       fi
       echo "AMPS initialization script installed to /etc/init.d/amps"
       update-rc.d amps defaults || echo "Couldn't update!"
       echo "Loaded AMPS service definition"
       ;;
     systemd)
       cp -f amps.service ${SYSTEMD_PATH}/amps.service
       ERROR=$?
       if [ ! -e ${SYSTEMD_PATH}/amps.service ]; then
          echo "Installation failed: $ERROR"
          exit $ERROR
       fi
       echo "AMPS service definition installed to /usr/lib/systemd/system/amps.service"
       systemctl daemon-reload
       echo "Loaded AMPS service definition"
       ;;
     chkconfig)
       echo "Setting up SYSV style service"
       cp -f amps-init-script-rhat /etc/init.d/amps
       ERROR=$?
       if [ ! -x /etc/init.d/amps ]; then
          echo "Installation failed: $ERROR."
          exit $ERROR
       fi
       echo "AMPS initialization script installed to /etc/init.d/amps"
       chkconfig --add amps  || echo "Couldn't add AMPS as a service"
       chkconfig --level 345 amps on || echo "Couldn't turn AMPS service on"
       echo "Configured AMPS service"
       ;;
esac

echo "Daemon is installed. You must provide a config file at /opt/etc/amps/config.xml"
