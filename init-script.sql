DROP TABLE IF EXISTS `fil_rouge`.`dw_weather`;
DROP TABLE IF EXISTS `fil_rouge`.`dw_station_means`;
DROP TABLE IF EXISTS `fil_rouge`.`dw_station_sampled`;
DROP TABLE IF EXISTS `fil_rouge`.`dw_station_state`;
DROP TABLE IF EXISTS `fil_rouge`.`dw_station`;
DROP TABLE IF EXISTS `fil_rouge`.`dw_city`;

# Cleaned city list
CREATE TABLE `fil_rouge`.`DW_city` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


# Cleaned static stations infos
CREATE TABLE `fil_rouge`.`DW_station` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `station_number` int(11) DEFAULT NULL,
  `city_id` int(11) DEFAULT NULL,
  `station_name` varchar(255) DEFAULT NULL,
  `address` varchar(255) DEFAULT NULL,
  `banking` tinyint(4) DEFAULT NULL,
  `bonus` tinyint(4) DEFAULT NULL,
  `latitude` float DEFAULT NULL,
  `longitude` float DEFAULT NULL,
  `elevation` float DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `city_id_idx` (`city_id`),
  CONSTRAINT `city_id` FOREIGN KEY (`city_id`) REFERENCES `DW_city` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


# Cleaned weather
CREATE TABLE `fil_rouge`.`DW_weather` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `city_id` int(11) DEFAULT NULL,
  `weather_group` varchar(45) DEFAULT NULL,
  `pressure` int(11) DEFAULT NULL,
  `humidity_percentage` float DEFAULT NULL,
  `temperature` float DEFAULT NULL,
  `min_temperature` float DEFAULT NULL,
  `max_temperature` float DEFAULT NULL,
  `wind_speed` float DEFAULT NULL,
  `wind_direction` float DEFAULT NULL,
  `cloudiness_percentage` int(11) DEFAULT NULL,
  `rain_quantity` int(11) DEFAULT NULL,
  `snow_quantity` int(11) DEFAULT NULL,
  `sun_set` int(11) DEFAULT NULL,
  `sun_rise` int(11) DEFAULT NULL,
  `calculation_time` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `city_id_idx` (`city_id`),
  CONSTRAINT `city_id2` FOREIGN KEY (`city_id`) REFERENCES `DW_city` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


# Cleaned states per station
CREATE TABLE `fil_rouge`.`DW_station_state` (
  `id` int(11) NOT NULL  AUTO_INCREMENT,
  `id_station` int(11) DEFAULT NULL,
  `status` tinyint(4) DEFAULT NULL,
  `operational_bike_stands` int(11) DEFAULT NULL,
  `available_bike_stands` int(11) DEFAULT NULL,
  `available_bikes` int(11) DEFAULT NULL,
  `last_update` bigint(20) DEFAULT NULL,
  `movements` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  INDEX station_time (id_station,last_update),
  KEY `id_station_idx` (`id_station`),
  CONSTRAINT `id_st` FOREIGN KEY (`id_station`) REFERENCES `DW_station` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


# Global means for each station, divided by day-time ranges
CREATE TABLE `fil_rouge`.`DW_station_means` (
  `id` int(11) NOT NULL  AUTO_INCREMENT,
  `id_station` int(11) NOT NULL,
  `week_day` int(3) DEFAULT NULL,
  `range_start` int(11) DEFAULT NULL,
  `range_end` int(11) DEFAULT NULL,
  `movement_mean` float DEFAULT NULL,
  `availability_mean` float DEFAULT NULL,
  `velib_nb_mean` float DEFAULT NULL,
  `movement_mean_rain` float DEFAULT NULL,
  `movement_mean_sun` float DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `id_station_idx` (`id_station`),
  CONSTRAINT `id_st2` FOREIGN KEY (`id_station`) REFERENCES `DW_station` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


# Sampled data table, generated from DW_station_state
CREATE TABLE `fil_rouge`.`DW_station_sampled` (
  `id` int(11) NOT NULL  AUTO_INCREMENT,
  `id_station` int(11) NOT NULL,
  `timestamp_start` int(11) DEFAULT NULL,
  `timestamp_end` int(11) DEFAULT NULL,
  `movement_mean` float DEFAULT NULL,
  `availability_mean` float DEFAULT NULL,
  `velib_nb_mean` float DEFAULT NULL,
  `weather` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`id`),
  INDEX station_time (id_station,timestamp_start),
  KEY `id_station_idx` (`id_station`),
  CONSTRAINT `id_st3` FOREIGN KEY (`id_station`) REFERENCES `DW_station` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
