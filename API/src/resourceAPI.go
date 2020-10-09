package main

import (
	_ "github.com/lib/pq"
	log "github.com/sirupsen/logrus"
)

// IncludeResourceAPIs includes all APIs described in this file in an APICollection
func IncludeResourceAPIs(c *APICollection) {
	getUsersForSharedAccountComputeResource := BaseAPI{
		InputModel{
			Parameter{ResourceName, true},
			Parameter{AccountName, true},
			Parameter{LastUpdated, false},
		},
		getUsersForSharedAccountComputeResource,
		RoleWrite,
	}
	c.Add("getUsersForSharedAccountComputeResource", &getUsersForSharedAccountComputeResource)

}

func getUsersForSharedAccountComputeResource(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	accountid := NewNullAttribute(UID)
	resourceid := NewNullAttribute(ResourceID)

	err := c.DBtx.QueryRow(`select (select uid from users where uname = $1 and is_groupaccount = 'true'),
								   (select compid from compute_resources where name = $2)`, i[AccountName], i[ResourceName]).Scan(&accountid, &resourceid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	} else if !accountid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorInvalidData, AccountName))
		return nil, apiErr
	} else if !resourceid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorInvalidData, ResourceName))
		return nil, apiErr
	}

	rows, err := c.DBtx.Query(`select u.uname, u.uid, c.is_leader
							   from users u
								 join compute_resource_shared_account c using (uid)
							   where c.groupaccount_uid = $1
							     and c.compid = $2
							     and (c.last_updated>=$3 or $3 is null)`, accountid, resourceid, i[LastUpdated])
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type useraccount map[Attribute]interface{}
	out := make([]useraccount, 0)

	for rows.Next() {
		row := NewMapNullAttribute(UserName, UID, Leader)
		rows.Scan(row[UserName], row[UID], row[Leader])

		if row[UID].Valid {
			entry := make(useraccount)
			entry[UserName] = row[UserName].Data
			entry[UID] = row[UID].Data
			entry[Leader] = row[Leader].Data
			out = append(out, entry)
		}
	}

	return out, nil
}
