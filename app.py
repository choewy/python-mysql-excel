import pymysql
import pandas as pd


class MySQL:
    def __init__(self, host: str = None,
                 port: int = 3306,
                 user: str = "root",
                 password: str = "password",
                 db: str = None) -> None:
        self.conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            db=db,
            autocommit=True,
            charset="utf8",
            cursorclass=pymysql.cursors.DictCursor
        )
        self.curs = self.conn.cursor()
        self._fields_format = {
            "field": "",
            "type": "",
            "null": True,
            "primary_key": False,
            "auto_increment": False
        }

    def create_database(self, db: str) -> {}:
        print("데이터베이스 생성 중 ...", end="\t")

        query = ["CREATE DATABASE", db + ";"]
        query = " ".join(query)

        response = {"success": True}

        try:
            self.conn.query(query)
            print("완료")
        except Exception as error:
            response["success"] = False
            response["error"] = error
            print(f"실패\t{error}")

        return response

    def use_database(self, db: str) -> {}:
        print("데이터베이스 변경 중 ...", end="\t")

        query = ["USE", db + ";"]
        query = " ".join(query)

        response = {"success": True}

        try:
            self.conn.query(query)
            self.curs = self.conn.cursor()
            print("완료")
        except Exception as error:
            print("존재하지 않음")
            do = self.create_database('db')
            if do['success']:
                self.use_database(db)
                print("완료")
            else:
                response["success"] = False
                response["error"] = error
                print(f"실패\t{error}")

        return response

    def fields_format(self):
        return self._fields_format

    def create_table(self, tbl: str, tbl_fields: []) -> {}:
        print("새로운 테이블 생성 중 ...", end="\t")

        query = ["CREATE TABLE " + f"{tbl}" + " ("]
        fields = []
        for idx, tbl_field in enumerate(tbl_fields):
            field = [f"{tbl_field['field']}",
                     tbl_field["type"]]

            if not tbl_field["null"]:
                field.append("NOT NULL")

            if tbl_field["auto_increment"]:
                field.append("AUTO_INCREMENT PRIMARY KEY")

            elif tbl_field["primary_key"]:
                field.append("PRIMARY KEY")

            fields.append(" ".join(field))

        query.append(", \n".join(fields) + ") CHARSET=utf8;")
        query = "\n".join(query)

        response = {"success": True}

        try:
            self.curs.execute(query)
            print("완료")
        except Exception as error:
            response["success"] = False
            response["error"] = error
            print(f"실패\t{error}")

        return response

    def insert_many(self, tbl: str, fields: [], rows: []):
        print("다중 데이터 입력 중 ...", end="\t")

        query = f"INSERT INTO {tbl}({', '.join(fields)}) VALUES ({', '.join(['%s' for _ in fields])})"

        response = {"success": True}

        try:
            self.curs.executemany(query, rows)
            print("완료")
        except Exception as error:
            response["success"] = False
            response["error"] = error
            print(f"실패\t{error}")

        return response

    def select_to_list(self, query):
        print("데이터 조회 중 ...", end="\t")
        response = {"success": True}

        try:
            self.curs.execute(query)
            response["rows"] = self.curs.fetchall()
            print("완료")
        except Exception as error:
            response["success"] = False
            response["error"] = error
            print(f"실패\t{error}")

        return response


class ExcelReader:
    def __init__(self, path: str, db: MySQL = None) -> None:
        self.path = path
        self.db = db

    def read_excel(self, sheet: int = 0) -> {}:
        print("엑셀 파일을 읽는 중 ...", end="\t")

        response = {"success": True}

        try:
            response["df"] = pd.read_excel(self.path, sheet_name=sheet).fillna("")
            print("완료")
        except Exception as error:
            response["success"] = False
            response["error"] = error
            print(f"실패\t{error}")

        return response

    def to_mysql(self, tbl: str, fields: [], rows: []) -> {}:
        self.db.create_table(tbl, fields)
        return self.db.insert_many(tbl, [field["field"] for field in fields], rows)


if __name__ == "__main__":
    """ EXCEL DATA TO MYSQL
    # mysql = MySQL("stdte")
    # file_path = "id_20220210.xlsx"
    #     new_tbl = "id_20220210"
    #     new_fields = [
    #         {
    #             "field": "no",
    #             "type": "VARCHAR(20)",
    #             "null": False,
    #             "auto_increment": False,
    #             "primary_key": False
    #         }, {
    #             "field": "cable_no",
    #             "type": "VARCHAR(100)",
    #             "null": False,
    #             "auto_increment": False,
    #             "primary_key": False
    #         }, {
    #             "field": "raceway_no",
    #             "type": "VARCHAR(100)",
    #             "null": False,
    #             "auto_increment": False,
    #             "primary_key": False
    #         }, {
    #             "field": "sequence",
    #             "type": "VARCHAR(10)",
    #             "null": False,
    #             "auto_increment": False,
    #             "primary_key": False
    #         }, {
    #             "field": "raceway_type",
    #             "type": "VARCHAR(100)",
    #             "null": False,
    #             "auto_increment": False,
    #             "primary_key": False
    #         }, {
    #             "field": "room_no",
    #             "type": "VARCHAR(20)",
    #             "null": False,
    #             "auto_increment": False,
    #             "primary_key": False
    #         }
    #     ]
    # excel_reader = ExcelReader(file_path, mysql)
    # res = excel_reader.read_excel()
    # if res["success"]:
    #     data = res["df"].values.tolist()
    #     excel_reader.to_mysql(new_tbl, new_fields, data)
    """

    mysql = MySQL(db="stdte")
    result = mysql.select_to_list("SELECT * FROM id_20220210;")

    if result["success"]:
        df = pd.DataFrame(result["rows"])
        print(df)