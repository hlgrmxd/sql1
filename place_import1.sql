CREATE OR REPLACE FUNCTION sync.place_import(_js JSON) RETURNS VOID
    SECURITY DEFINER
    LANGUAGE plpgsql
AS
$$
BEGIN
    SET TIME ZONE 'Europe/Moscow';
    IF _js IS NULL THEN
        RETURN;
    END IF;

    WITH cte  AS (SELECT place_id, place_name, dt, is_del, wh_id, stage, steet, section, ROW_NUMBER() OVER (PARTITION BY place_id ORDER BY dt DESC ) rn
                 FROM JSON_TO_RECORDSET(_js)  src(place_id INT,
                                              place_name VARCHAR(100),
                                              dt TIMESTAMP, --последняя дата изменения
                                              is_del BOOL, --признак удаления TRUE - удален
                                              wh_id INT, --номер блока где расположено мх
                                              stage INT, --этаж
                                              steet INT, --улица
                                              section INT) /*--секция)*/)

         -- обновляем запись, если она уже существует в таблице
    INSERT
    INTO wh.storageplace AS sp(place_id, place_name, dt, employee_id, is_del, wh_id, stage, steet, section)
    SELECT c.place_id, с.place_name, c.dt, c.is_del, c.wh_id, c.stage, c.steet, c.section
    FROM cte c
    WHERE c.rn = 1
    ON CONFLICT (place_id) DO UPDATE
        SET place_name  = excluded.place_name,
            dt          = excluded.dt,
            employee_id = excluded.employee_id,
            is_del      = excluded.is_del,
            wh_id       = excluded.wh_id,
            stage       = excluded.stage,
            steet       = excluded.steet,
            section     = excluded.section
    WHERE excluded.place_name!=sp.place_name AND excluded.dt > sp.dt;
END
$$;


SELECT * FROM sync.place_import('[
               {
                 "place_id": 123,
                 "place_name": "123123",
                 "dt": "2023-06-10 13:16:58.631596 +00:00",
                 "is_del": false,
                 "wh_id": 1,
                 "stage": 1,
                 "steet": 1,
                 "section": 1
               },
               {
                 "place_id": 123,
                 "place_name": "321321",
                 "dt": "2023-06-10 16:16:58.631596 +00:00",
                 "is_del": false,
                 "wh_id": 1,
                 "stage": 1,
                 "steet": 1,
                 "section": 1
               },
               {
                 "place_id": 124,
                 "place_name": "123124",
                 "dt": "2023-06-10 12:16:58.631596 +00:00",
                 "is_del": true,
                 "wh_id": 1,
                 "stage": 1,
                 "steet": 1,
                 "section": 1
               },
               {
                 "place_id": 124,
                 "place_name": "123124",
                 "dt": "2023-06-10 13:16:58.631596 +00:00",
                 "is_del": true,
                 "wh_id": 1,
                 "stage": 1,
                 "steet": 1,
                 "section": 1
               },
               {
                 "place_id": 124,
                 "place_name": "123124",
                 "dt": "2023-06-10 20:16:58.631596 +00:00",
                 "is_del": true,
                 "wh_id": 1,
                 "stage": 1,
                 "steet": 1,
                 "section": 1
               }
             ]'::JSON);

SELECT * FROM wh.storageplace