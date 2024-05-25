package storage

import (
	"log"
	"sync"
	"time"

	"github.com/AnimeKaizoku/cacher"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

type PeerStorage struct {
	peerCache  *cacher.Cacher[int64, *Peer]
	peerLock   *sync.RWMutex
	inMemory   bool
	SqlSession *gorm.DB
}

type options struct {
	tablePrefix *string
}

func WithTablePrefix(tableName *string) func(*options) {
	return func(opts *options) {
		opts.tablePrefix = tableName
	}
}

func NewPeerStorage(dialector gorm.Dialector, inMemory bool, optsF ...func(*options)) *PeerStorage {
	o := options{}
	for _, optF := range optsF {
		optF(&o)
	}

	p := PeerStorage{
		inMemory: inMemory,
		peerLock: new(sync.RWMutex),
	}
	var opts *cacher.NewCacherOpts
	if inMemory {
		opts = nil
	} else {
		opts = &cacher.NewCacherOpts{
			TimeToLive:    6 * time.Hour,
			CleanInterval: 24 * time.Hour,
			Revaluate:     true,
		}

		gormConfig := &gorm.Config{
			SkipDefaultTransaction: true,
			Logger:                 logger.Default.LogMode(logger.Silent),
		}

		if o.tablePrefix != nil {
			gormConfig.NamingStrategy = schema.NamingStrategy{
				TablePrefix: *o.tablePrefix,
			}
		}

		db, err := gorm.Open(dialector, gormConfig)
		if err != nil {
			log.Panicln(err)
		}
		p.SqlSession = db
		dB, _ := db.DB()
		dB.SetMaxOpenConns(100)
		_ = p.SqlSession.AutoMigrate(&Session{}, &Peer{})
	}
	p.peerCache = cacher.NewCacher[int64, *Peer](opts)
	return &p
}
