package dbrpc

import (
	"database/sql"
	"github.com/go-siris/siris/core/errors"
	mysql "mqtt/cores/dcache/mysql"
	"mqtt/cores/dcache/rpc_def"
	"mqtt/utils/zlog"
	"sync"
)

type MysqlDataCenterUserRPC struct {
	lock *sync.RWMutex
}

func InitMysqlDataCemterUserRPC() *MysqlDataCenterUserRPC {
	return &MysqlDataCenterUserRPC{
		new(sync.RWMutex),
	}
}

//举一个注册的例子
func (f *MysqlDataCenterUserRPC) Register(args *rpc_def.ArgsRegisterData, ret *rpc_def.ReplyRegisterInfo) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	db := mysql.GetMysqlDB(mysql.DB_DataCenterPlatform)
	//先查一下 有没有这个用户
	sqlStr := "SELECT id FROM SYSTEM_user_info WHERE user_account = ? and user_password = ? LIMIT 1"
	rows := db.QueryRow(sqlStr, args.UserAccount, args.UserPassword)
	err := rows.Scan(&ret.Id)
	//if ErrNoRows
	if err == sql.ErrNoRows {
		//增加数据到 用户表
		var id int64
		if db != nil {
			sqlStr := "INSERT INTO  SYSTEM_user_info (user_account,user_password,user_type) VALUES (?,?,?)"
			res, err := db.Exec(sqlStr, &args.UserAccount, &args.UserPassword, &args.UserType)
			id, err = res.LastInsertId()
			if err != nil {
				zlog.Errorf("MysqlDataCenterUserRPC.Register error: %+v", err)
				return err
			}
			ret.Id = id
		} else {
			zlog.Error("MysqlDataCenterUserRPC.Register error: db is nil")
			return err
		}
	} else {
		zlog.Errorf("MysqlDataCenterUserRPC.Register user is exist , args.UserAccount: %s", args.UserAccount)
		return errors.New("user is exist")
	}
	return nil
}