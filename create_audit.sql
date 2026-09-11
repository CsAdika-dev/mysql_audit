DELIMITER $$

DROP PROCEDURE IF EXISTS `create_audit`$$

CREATE PROCEDURE `create_audit`(
    IN `schemaName` VARCHAR(255), 
    IN `auditTable` VARCHAR(255), 
    IN `triggerSuffix` VARCHAR(255)
)
BEGIN
    DECLARE tableName, columnName, triggerName VARCHAR(255) DEFAULT "";
    DECLARE triggerHandler, tableHandler, columnHandler INT DEFAULT 0;
    DECLARE jsonFieldsOld, jsonFieldsNew TEXT DEFAULT "";

 15 |     -- Extend GROUP_CONCAT length (10MB) to avoid truncating large DDL
    SET SESSION group_concat_max_len = 10485760;

    DROP TEMPORARY TABLE IF EXISTS ddlTable;
    CREATE TEMPORARY TABLE ddlTable (id INT NOT NULL AUTO_INCREMENT, command TEXT, PRIMARY KEY (id));

    INSERT INTO ddlTable (command) VALUES (CONCAT("USE `", schemaName, "`;\n"));

 23 |     -- 1. Drop existing triggers
    BlockDropTrigger: BEGIN
        DECLARE triggerCursor CURSOR FOR 
            SELECT TRIGGER_NAME 
            FROM INFORMATION_SCHEMA.TRIGGERS 
            WHERE TRIGGER_SCHEMA = schemaName AND UPPER(TRIGGER_NAME) LIKE CONCAT("%_", UPPER(triggerSuffix));
        DECLARE CONTINUE HANDLER FOR NOT FOUND SET triggerHandler = 1;

        OPEN triggerCursor;
        triggerLoop: LOOP
            FETCH triggerCursor INTO triggerName;
            IF triggerHandler THEN
                LEAVE triggerLoop;
            END IF;
            INSERT INTO ddlTable (command) VALUES (CONCAT("DROP TRIGGER IF EXISTS `", triggerName, "`;"));
        END LOOP triggerLoop;
        CLOSE triggerCursor;
    END BlockDropTrigger;

 42 |     -- 2. Create audit table DDL
    SET @createTable = CONCAT("CREATE TABLE IF NOT EXISTS `", auditTable, "` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `table_name` VARCHAR(255) NOT NULL,
  `old_row_data` JSON DEFAULT NULL,
  `new_row_data` JSON DEFAULT NULL,
  `dml_type` ENUM('INSERT','UPDATE','DELETE') NOT NULL,
  `dml_timestamp` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `dml_created_by` VARCHAR(255) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB;\n");
    INSERT INTO ddlTable (command) VALUES (@createTable);

 55 |     -- 3. Generate triggers per table
    BlockTable: BEGIN
        DECLARE tableCursor CURSOR FOR 
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = schemaName 
              AND TABLE_TYPE = 'BASE TABLE' 
              AND TABLE_NAME != auditTable;
        DECLARE CONTINUE HANDLER FOR NOT FOUND SET tableHandler = 1;

        OPEN tableCursor;
        tableLoop: LOOP
            FETCH tableCursor INTO tableName;
            IF tableHandler THEN
                LEAVE tableLoop;
            END IF;

            SET jsonFieldsOld = "";
            SET jsonFieldsNew = "";

 75 |             -- Concatenate fields as parameters for JSON_OBJECT
            BlockColumn: BEGIN
                DECLARE columnCursor CURSOR FOR 
                    SELECT COLUMN_NAME 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_SCHEMA = schemaName AND TABLE_NAME = tableName 
                    ORDER BY ORDINAL_POSITION;
                DECLARE CONTINUE HANDLER FOR NOT FOUND SET columnHandler = 1;

                OPEN columnCursor;
                columnLoop: LOOP
                    FETCH columnCursor INTO columnName;
                    IF columnHandler THEN
                        SET columnHandler = 0;
                        LEAVE columnLoop;
                    END IF;

                    IF LENGTH(jsonFieldsOld) > 0 THEN
                        SET jsonFieldsOld = CONCAT(jsonFieldsOld, ", ");
                        SET jsonFieldsNew = CONCAT(jsonFieldsNew, ", ");
                    END IF;

                    SET jsonFieldsOld = CONCAT(jsonFieldsOld, "'", columnName, "', OLD.`", columnName, "`");
                    SET jsonFieldsNew = CONCAT(jsonFieldsNew, "'", columnName, "', NEW.`", columnName, "`");
                END LOOP columnLoop;
                CLOSE columnCursor;
            END BlockColumn;

103 |             -- INSERT Trigger
            INSERT INTO ddlTable (command) VALUES (CONCAT(
                "CREATE TRIGGER `", tableName, "_insert_", triggerSuffix, "` AFTER INSERT ON `", tableName, "`\n",
                "FOR EACH ROW\nBEGIN\n",
                "    INSERT INTO `", auditTable, "` (`table_name`, `old_row_data`, `new_row_data`, `dml_type`, `dml_created_by`)\n",
                "    VALUES ('", tableName, "', NULL, JSON_OBJECT(", jsonFieldsNew, "), 'INSERT', USER());\n",
                "END;\n"
            ));

112 |             -- UPDATE Trigger
            INSERT INTO ddlTable (command) VALUES (CONCAT(
                "CREATE TRIGGER `", tableName, "_update_", triggerSuffix, "` AFTER UPDATE ON `", tableName, "`\n",
                "FOR EACH ROW\nBEGIN\n",
                "    INSERT INTO `", auditTable, "` (`table_name`, `old_row_data`, `new_row_data`, `dml_type`, `dml_created_by`)\n",
                "    VALUES ('", tableName, "', JSON_OBJECT(", jsonFieldsOld, "), JSON_OBJECT(", jsonFieldsNew, "), 'UPDATE', USER());\n",
                "END;\n"
            ));

121 |             -- DELETE Trigger
            INSERT INTO ddlTable (command) VALUES (CONCAT(
                "CREATE TRIGGER `", tableName, "_delete_", triggerSuffix, "` AFTER DELETE ON `", tableName, "`\n",
                "FOR EACH ROW\nBEGIN\n",
                "    INSERT INTO `", auditTable, "` (`table_name`, `old_row_data`, `new_row_data`, `dml_type`, `dml_created_by`)\n",
                "    VALUES ('", tableName, "', JSON_OBJECT(", jsonFieldsOld, "), NULL, 'DELETE', USER());\n",
                "END;\n"
            ));

        END LOOP tableLoop;
        CLOSE tableCursor;
    END BlockTable;

134 |     -- Return output
    SELECT GROUP_CONCAT(command SEPARATOR "\n") AS run_this_ddl FROM ddlTable ORDER BY id;
END$$

DELIMITER ;