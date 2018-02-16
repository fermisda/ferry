
class CollaborationUnit:
    def __init__(self, name):
        self.name = name
        self.alt_name = None
        self.groups ={}
        self.unitid = 0

    def add_unix_gid(self, gname, gid):
        self.groups[gname] = gid

    def set_alt_name(self,name):
        self.alt_name = name

    def set_id(self, index):
        self.unitid = index

class VOMS(CollaborationUnit):
    """
    VOMS presents information about VOMS instance groups and roles
    voname is a name of VOMS
    experiment could be either a VO name or subgroup as in case of fermilab VO
    It is planning to address any generic case it just deal with what we have now
    """
    def __init__(self, vurl, vo_name, experiment):
        CollaborationUnit.__init__(self,experiment)
        self.url = [vurl]
        if experiment != vo_name:
            self.url = "%s/%s" % (vurl, experiment)
        self.roles = []
        self.voms_gid = None

    def add_voms_unix_group(self, gname,gid):
        self.voms_gid = gid
        self.add_unix_gid(gname,gid)

    def add_roles(self, rnames):
        self.roles = rnames

    def add_voms_url(self, vurl):
        self.url.append(vurl)

class VOUserGroup:
    """
    GUMS mapping from configuration file
    """

    def __init__(self, voms_user_group, voms_server, vo_group, role):
        self.user_group = voms_user_group
        self.server = voms_server
        self.group = vo_group
        self.role = role
        self.uname = None
        self.account_mappers = None
        self.gid = None
        self.fqanid = 0

    def set_id(self, index):
        self.fqanid = index


class ComputeResource:
    def __init__(self, name, collaboration, compute_type = "Interactive", default_home = "/nashome", default_shell = "/bin/bash"):
        self.cresource = name
        self.cunit = collaboration
        self.ctype = compute_type
        self.chome = default_home
        self.cshell = default_shell
        self.unitid = None
        self.cid = None
        self.users = {}
        self.groups = {}
        self.primary_gid = []
        self.alternative_name = None
        self.batch = []

class ComputeBatch:
    def __init__(self, name, value = None, type = None, groupid = None):
        self.name = name
        self.value = value
        self.type = type
        self.groupid = groupid


class StorageResource:
    def __init__(self, name, storage_type, default_path, default_quota, unit):
        self.sresource = name
        self.stype = storage_type
        self.spath = default_path
        self.squota = default_quota
        self.sunit = unit
        self.quotas = []

class StorageQuota:
    def __init__(self, uid, gid, cunit, path, value, unit, valid_until):
        self.quid = uid
        self.qgid = gid
        self.qcunit = cunit
        self.qpath = path
        self.qvalue = value
        self.qunit = unit
        self.quntil = valid_until

    def add_mapping(self,name, altname):
        self.alternative_name  = altname

class NasStorage:
    def __init__(self, server, volume, access_level, host):
        self.server = server
        self.volume = volume
        self.access_level = access_level
        self.host = host
