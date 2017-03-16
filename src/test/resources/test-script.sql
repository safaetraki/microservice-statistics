delete from `fil_rouge`.`dw_station_means`;
delete from `fil_rouge`.`dw_station_sampled`;
delete from `fil_rouge`.`dw_station_state`;
delete from `fil_rouge`.`dw_station`;
delete from `fil_rouge`.`dw_weather`;
delete from `fil_rouge`.`dw_city`;

INSERT INTO `fil_rouge`.`dw_city`
(`id`,
`name`)
VALUES
(1,
"city1");

INSERT INTO `fil_rouge`.`dw_station`
(`id`,
`station_number`,
`city_id`,
`station_name`,
`address`,
`banking`,
`bonus`,
`latitude`,
`longitude`,
`elevation`)
VALUES
(1,
1,
1,
"station1",
"",
0,
0,
0,
1,
1);

INSERT INTO `fil_rouge`.`dw_station_state`
(`id`,
`id_station`,
`status`,
`operational_bike_stands`,
`available_bike_stands`,
`available_bikes`,
`last_update`,
`movements`)
VALUES
(1,
1,
0,
5,
2,
3,
1488711600,
null);

INSERT INTO `fil_rouge`.`dw_station_state`
(`id`,
`id_station`,
`status`,
`operational_bike_stands`,
`available_bike_stands`,
`available_bikes`,
`last_update`,
`movements`)
VALUES
(2,
1,
0,
5,
3,
2,
1488711720,
null);

INSERT INTO `fil_rouge`.`dw_station_state`
(`id`,
`id_station`,
`status`,
`operational_bike_stands`,
`available_bike_stands`,
`available_bikes`,
`last_update`,
`movements`)
VALUES
(3,
1,
0,
5,
4,
4,
1488711840,
null);

INSERT INTO `fil_rouge`.`dw_weather`
(`id`,
`city_id`,
`weather_group`,
`calculation_time`)
VALUES
(1,
1,
'rain',1488711600);
