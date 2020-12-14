"""This module provides static methods to establish connection and excute queries against mysql database. """

import tempfile
import sys
import os
import subprocess
import shlex

class MySQLUtils:
    """Provides utility methods to deal with mysql database. """

    @staticmethod
    def createClientConfig(dbn,config):
        """
        Creates temp password file
        Args:
            dbn: (str) - configuration segment name that describes the database
            config: Configuration object

        Returns:

        """
        name=None
        try:
            dbPasswd=config.get(dbn,"password")
            fd,name=tempfile.mkstemp(prefix='.mysql')
            os.write(fd, str.encode("[client]\n"))
            os.write(fd,str.encode('password="%s"\n' % (dbPasswd,)))
            os.close(fd)
        except:
            print("Didn't create client configuration file", sys.exc_info()[0], file=sys.stderr)
        return name

    @staticmethod
    def getDbConnection(dbn,tmpPwdFile,config):
        """
        Creates db connection string
        Args:
            dbn: configuration segment name that describes the database
            tmpPwdFile: temporary password file name
            config: Configuration object

        Returns:

        """
        options=""
        try:
            mysql  = config.get("path", "mysql")
            dbHost = config.get(dbn, "hostname")
            dbUser = config.get(dbn, "username")
            dbPort = config.get(dbn, "port")
            dbName = config.get(dbn, "schema")
        except:
            print("ERROR!!! The " + dbn + " section either does not exist or does not contain all the needed information or has an error in it.", file=sys.stderr)
            MySQLUtils.removeClientConfig(tmpPwdFile)
            sys.exit(1)
        if tmpPwdFile!=None:
            options="--defaults-extra-file=" + tmpPwdFile
        return mysql + " " + options + " -h " + dbHost + " -u " + dbUser + " --port=" + dbPort + " -N " +  dbName

    @staticmethod
    def RunQuery(select,connectString,verbose=False):
        """
        Assembles mysql command and runs the query
        Args:
            select: select statment
            connectString: mysql connection parameters
            verbose:

        Returns:

        """

        command_line="%s -e \"%s\"" % (connectString,select)
        return MySQLUtils.executeCmd(command_line,verbose)

    @staticmethod
    def executeCmd(cmd,verbose=False):
        """
        Executes mysql command
        Args:
            cmd: mysql commnand
            verbose:

        Returns:

        """
        if verbose:
            print >> sys.stdout, cmd
        proc = subprocess.Popen(cmd,shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE, universal_newlines=True)
        # Reads from pipes, avoides blocking
        result,error=proc.communicate()
        return_code = proc.wait()
        if verbose:
            print(result, file=sys.stderr)
            print(error, file=sys.stderr)
        if verbose:
            print("command return code is %s" % (return_code,), file=sys.stderr)
        return result.strip().split("\n"),return_code
 
    @staticmethod
    def removeClientConfig(tmpPwdFile):
        """Removes mysql client config file
        Args:
		tmpPwdFile (str) - name of password file
		"""
        try:
            if tmpPwdFile != None:
                os.unlink(tmpPwdFile)
        except:
            pass
