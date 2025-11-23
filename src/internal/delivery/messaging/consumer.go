package messaging

import (
	"order-service/src/pkg/log"
	"order-service/src/pkg/redis"
)

func setConfluentEvents() {
	redisClient := redis.GetClient()
	kafkaProducer, err := kafkaConfluent.NewProducer(kafkaConfluent.GetConfig().GetKafkaConfig(), log.GetLogger())
	if err != nil {
		panic(err)
	}
	passangerQueryMongoRepo := passangerRepoQueries.NewQueryMongodbRepository(mongodb.NewMongoDBLogger(mongodb.GetSlaveConn(), mongodb.GetSlaveDBName(), log.GetLogger()))
	passangerCommandRepo := passangerRepoCommands.NewCommandMongodbRepository(mongodb.NewMongoDBLogger(mongodb.GetSlaveConn(), mongodb.GetSlaveDBName(), log.GetLogger()))
	passangerCommandUsecase := passangerUsecase.NewCommandUsecase(passangerQueryMongoRepo, passangerCommandRepo, redisClient, kafkaProducer)
	passangerConsumer, errPassanger := kafkaConfluent.NewConsumer(kafkaConfluent.GetConfig().GetKafkaConfig(), log.GetLogger())

	//
	driverQueryMongoRepo := driverRepoQueries.NewQueryMongodbRepository(mongodb.NewMongoDBLogger(mongodb.GetSlaveConn(), mongodb.GetSlaveDBName(), log.GetLogger()))
	driverCommandRepo := driverRepoCommands.NewCommandMongodbRepository(mongodb.NewMongoDBLogger(mongodb.GetSlaveConn(), mongodb.GetSlaveDBName(), log.GetLogger()))
	driverCommandUsecase := driverUsecase.NewCommandUsecase(driverQueryMongoRepo, driverCommandRepo, redisClient, kafkaProducer)
	driverConsumer, errDriver := kafkaConfluent.NewConsumer(kafkaConfluent.GetConfig().GetKafkaConfig(), log.GetLogger())

	passangerHandler.InitPassangerEventHandler(passangerCommandUsecase, passangerConsumer)
	driverHandler.InitPassangerEventHandler(driverCommandUsecase, driverConsumer)

	if errPassanger != nil {
		log.GetLogger().Error("main", "error registerNewConsumer", "setConfluentEvents", errPassanger.Error())
	}

	if errDriver != nil {
		log.GetLogger().Error("main", "error registerNewConsumer", "setConfluentEvents", errDriver.Error())
	}
}
