import sys

class ComputeAccess:
    def __init__(self, cname, gid, homedir, shell="/bin/bash" ):
        self.comp_name = cname
        self.gid = gid
        self.shell = shell
        self.home_dir = homedir
        self.secondary_groups = []

    def add_secondary_group(self, gid):
        self.secondary_groups.append(gid)


class UserGroup:
    def __init__(self, name, gid=0, leader=False):
        self.gname = name
        self.group_id = 0
        self.gid = gid
        self.is_leader = leader
        self.group_type = None


class AffiliationAttribute:
    def __init__(self, eid, name, value):
        self.experiment_id = eid
        self.attr_name = name
        self.attr_value = value


class Certificate:
    def __init__(self, eid, subject, issuer):
        self.experiment_id = eid
        self.dn = subject.replace("'","''")
        self.ca = issuer



class User:
    def __init__(self, uid, full_name, name):

        self.uname = name
        self.full_name = full_name.replace("'", "''")
        self.uid = uid
        self.status = True
        self.expiration_date = None
        self.is_k5login = False
        self.user_affiliation_attributes = {}
        self.groups = {}
        self.grid_access= {}
        self.compute_access = {}
        self.vo_membership = {}
        self.certificates = {}
        self.external_affiliations = []

    def add_to_vo(self, vname, vurl):
        if not self.vo_membership.__contains__((vname, vurl)):
             self.vo_membership[(vname, vurl)] = []

    def add_to_vo_role(self, vname, vurl, gums_mapping):
        for gm in self.vo_membership[(vname, vurl)]:
            if gm.group == gums_mapping.group and gums_mapping.role == gm.role:
                return
        self.vo_membership[(vname, vurl)].append(gums_mapping)

    def set_expiration_date(self, dt):
        self.expiration_date = dt

    def set_status(self, status):
        self.status = status

    def add_cert(self, cert):
        if cert.dn not in self.certificates.keys():
            self.certificates[cert.dn]={'ca': cert.ca, 'experiments': []}
            self.certificates[cert.dn]['experiments'].append(cert.experiment_id)
        else:
            if cert.ca != self.certificates[cert.dn]['ca']:
                print("Conflicting issuer CA information on certificate %s:\n%s != %s" %
                (cert.dn, self.certificates[cert.dn]['ca'], cert.ca))
            if cert.experiment_id not in self.certificates[cert.dn]['experiments']:
                self.certificates[cert.dn]['experiments'].append(cert.experiment_id)

    def add_external_affiliation(self, attribute, value):
        if (attribute, value) not in self.external_affiliations:
            self.external_affiliations.append((attribute, value))

    def get_certificate_for_experiments(self, eid):
        if eid in self.certificates:
            return self.certificates[eid]
        return []

    def add_attribute(self, attr):
        if attr.experiment_id not in self.user_affiliation_attributes:
            self.user_affiliation_attributes[attr.experiment_id]=[]
            self.user_affiliation_attributes[attr.experiment_id].append(attr)
        else:
            for a in self.user_affiliation_attributes[attr.experiment_id]:
                if a.attribute_name != self.user_affiliation_attributes[attr.experiment_id].attribute_name:
                    self.user_affiliation_attributes[attr.experiment_id].append(attr)
                    break
        return

    def get_user_affiliation_attributes_for_experiments(self, eid):
        if eid in self.user_affiliation_attributes:
            return self.user_affiliation_attributes[eid]
        return []

    def add_user_group(self, gr):
        if gr.group_id not in self.groups:
            self.certificates[gr.group_id] = gr
        return

    def add_group(self, name, gid, leader=False):
        for g in self.groups:
            if str(gid) == str(g):
                return
        self.groups[str(gid)] = UserGroup(name, str(gid), leader)

    def get_groups(self):
        gnames = []
        for g_id, group in self.groups.items():
            gnames.append(group.gname)
        return gnames

    def is_leader(self,groupid):
        if groupid in self.groups:
            return self.groups[groupid].is_leader
        else:
            return False

    def set_leader(self,groupid):
        if groupid in self.groups:
            self.groups[groupid].is_leader = True
        else:
            print("User %s is not a member of group with id %d" % (self.uname,groupid), file=sys.stderr)

    def get_experiment(self):
        enames = []
        for e_id, exp in self.grid_access.items():
            enames.append(exp.gname)
        return enames

    def is_banned(self, eid):
        if eid in self.grid_access():
            return self.grid_access[eid].is_banned
        else:
            return False

    def set_banned(self,eid):
        if eid in self.grid_access():
            self.grid_access[eid].is_banned = True
        else:
            print >> sys.stderr, "User %s is not a member of experiment with id %d" % (self.uname,eid)
        return

    def is_superuser(self, eid):
        if eid in self.grid_access():
            return self.grid_access[eid].is_superuser
        else:
            return False

    def set_shell(self,eid,shell):
        if eid in self.compute_access():
            self.compute_access[eid].shell = shell
        else:
            print >> sys.stderr, "User %s is not a member of experiment with id %d" % (self.uname,eid)
        return

    def set_homedir(self,eid, homedir):
        if eid in self.compute_access():
            self.compute_access[eid].home_dir = homedir
        else:
            print >> sys.stderr, "User %s is not a member of experiment with id %d" % (self.uname,eid)
        return