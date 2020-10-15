package main

import (
	"errors"

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
		RoleRead,
	}
	c.Add("getUsersForSharedAccountComputeResource", &getUsersForSharedAccountComputeResource)

	addUserToSharedAccountComputeResource := BaseAPI{
		InputModel{
			Parameter{UserName, true},
			Parameter{ResourceName, true},
			Parameter{AccountName, true},
			Parameter{Leader, false},
		},
		addUserToSharedAccountComputeResource,
		RoleWrite,
	}
	c.Add("addUserToSharedAccountComputeResource", &addUserToSharedAccountComputeResource)

	removeUserFromSharedAccountComputeResource := BaseAPI{
		InputModel{
			Parameter{UserName, true},
			Parameter{ResourceName, true},
			Parameter{AccountName, true},
		},
		removeUserFromSharedAccountComputeResource,
		RoleWrite,
	}
	c.Add("removeUserFromSharedAccountComputeResource", &removeUserFromSharedAccountComputeResource)

	getSharedAccountForComputeResource := BaseAPI{
		InputModel{
			Parameter{ResourceName, true},
		},
		getSharedAccountForComputeResource,
		RoleRead,
	}
	c.Add("getSharedAccountForComputeResource", &getSharedAccountForComputeResource)

	setSharedAccountComputeResourceApprover := BaseAPI{
		InputModel{
			Parameter{UserName, true},
			Parameter{ResourceName, true},
			Parameter{AccountName, true},
			Parameter{Leader, false},
		},
		setSharedAccountComputeResourceApprover,
		RoleWrite,
	}
	c.Add("setSharedAccountComputeResourceApprover", &setSharedAccountComputeResourceApprover)

	getShareAccountComputeResourceApprovers := BaseAPI{
		InputModel{
			Parameter{ResourceName, true},
			Parameter{AccountName, true},
		},
		getShareAccountComputeResourceApprovers,
		RoleRead,
	}
	c.Add("getShareAccountComputeResourceApprovers", &getShareAccountComputeResourceApprovers)

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

func addUserToSharedAccountComputeResource(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	accountid := NewNullAttribute(UID)
	resourceid := NewNullAttribute(ResourceID)
	uid := NewNullAttribute(UID)
	leader := i[Leader].Default(false)

	err := c.DBtx.QueryRow(`select (select uid from users where uname = $1 and is_groupaccount = 'true'),
								   (select compid from compute_resources where name = $2),
								   (select uid from users where uname = $3 and is_groupaccount = 'false')`,
		i[AccountName], i[ResourceName], i[UserName]).Scan(&accountid, &resourceid, &uid)
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

	_, err = c.DBtx.Exec(`insert into compute_resource_shared_account (groupaccount_uid, uid, compid, is_leader, last_updated)
							values ($1, $2, $3, $4, NOW())
							on conflict (groupaccount_uid, uid, compid) do update set is_leader = $4`,
		accountid, uid, resourceid, leader)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}

func removeUserFromSharedAccountComputeResource(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	accountid := NewNullAttribute(UID)
	resourceid := NewNullAttribute(ResourceID)
	uid := NewNullAttribute(UID)

	err := c.DBtx.QueryRow(`select (select uid from users where uname = $1 and is_groupaccount = 'true'),
								   (select compid from compute_resources where name = $2),
								   (select uid from users where uname = $3 and is_groupaccount = 'false')`,
		i[AccountName], i[ResourceName], i[UserName]).Scan(&accountid, &resourceid, &uid)
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
	} else if !uid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorInvalidData, UserName))
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`delete from compute_resource_shared_account where groupaccount_uid = $1 and uid = $2 and compid = $3`, &accountid, &uid, &resourceid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}

func setSharedAccountComputeResourceApprover(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	accountid := NewNullAttribute(UID)
	resourceid := NewNullAttribute(ResourceID)
	uid := NewNullAttribute(UID)
	leader := i[Leader].Default(false)

	err := c.DBtx.QueryRow(`select (select uid from users where uname = $1 and is_groupaccount = 'true'),
								   (select compid from compute_resources where name = $2),
								   (select uid from users where uname = $3 and is_groupaccount = 'false')`,
		i[AccountName], i[ResourceName], i[UserName]).Scan(&accountid, &resourceid, &uid)
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
	} else if !uid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorInvalidData, UserName))
		return nil, apiErr
	}

	res, err := c.DBtx.Exec(`update compute_resource_shared_account set leader = $1 where groupaccount_uid = $2 and uid = $3 and compuid = $4`, &leader, &accountid, &uid, &resourceid)
	aRows, _ := res.RowsAffected()
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	} else if aRows == 0 {
		apiErr = append(apiErr, APIError{errors.New("username is not a member of the group account for the resourcename"), ErrorAPIRequirement})
		return nil, apiErr
	}

	return nil, nil
}

func getSharedAccountForComputeResource(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	return nil, apiErr
}

func getShareAccountComputeResourceApprovers(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	return nil, apiErr
}
