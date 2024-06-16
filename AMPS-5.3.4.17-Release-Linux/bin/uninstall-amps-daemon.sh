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

# Purpose: This script uninstalls the 60East Technologies AMPS daemon.

INSTALLPATH=/opt/amps

## Capability check for installing things as daemons
##
## Rather than trying to figure out all of the permutations on which systems
## have particular ways of starting things, test for the ways this script
## knows how to set things up.
##

STYLE="Unknown"

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
   echo "Using Debian-style service installation."
fi

# Prefer systemd over chkconfig when available.

if [ ! -z `which chkconfig` ] && [ "$STYLE" = 'Unknown' ]; then
   STYLE="chkconfig" 
   echo "Using RedHat-style service installation and SYSV-style scripts"
fi
exec 2>&3

## Those are the styles we know how to detect. Provide output.

if [ -z "$STYLE" ]; then
  echo "$0 does not have an installation target for this system. Please provide details of your system and version to 60East support."
  exit 1;
fi

# Delete init script into location and uninstall it.

echo "Stop/Remove/Uninstall amps init script"

case $STYLE in
    update*)
      /etc/init.d/amps stop
      update-rc.d amps remove
      rm -f /etc/init.d/amps
      ;;
    systemd)
     systemctl stop amps.service
     rm -f ${SYSTEMD_PATH}/amps.service
     systemctl daemon-reload
     ;;
    chkconfig)
      /etc/init.d/amps stop
      chkconfig --del amps
      rm -f /etc/init.d/amps
      ;;
esac

echo "Deleting AMPS distribution"

rm -rf $INSTALLPATH

echo "There may still be data in /var/run/amps, we didn't delete that or the config file at /opt/etc/config.xml."
