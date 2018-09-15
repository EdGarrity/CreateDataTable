#pragma once

#include <boost/serialization/string.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/map.hpp>

#define NOMINMAX

#include <windows.h>
#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
#define _SQLNCLI_ODBC_

#include "C:\\Program Files\\Microsoft SQL Server\\110\\SDK\\Include\\sqlncli.h";
#include "..\Utilities\Singleton.h";

//#include <string>
//#include <vector>
//#include <map>

namespace database
{
//class Individual;
//class Tournament;

class SqlConnection
{
public:
	SqlConnection();
	~SqlConnection();

	struct Field
	{
		DWORD type;
		SQLLEN length;
		std::string value;

		union DataType
		{
		// Fix Later            //SQL_UNKNOWN_TYPE      //0
		SQLCHAR SqlChar[8000];  //SQL_CHAR              //1
		SQLCHAR SqlBit;			//SQL_CHAR              //1
		// Fix Later            //SQL_NUMERIC           //2
		// Fix Later            //SQL_DECIMAL           //3
		SQLINTEGER SqlInteger;  //SQL_INTEGER           //4
		// Fix Later            //SQL_SMALLINT          //5
		SQLFLOAT SqlFloat;      //SQL_FLOAT             //6
		//SQLREAL SqlReal;        //SQL_REAL              //7
		// Fix Later            //SQL_DOUBLE            //8
		SQL_TIMESTAMP_STRUCT SqlDateTime;	//SQL_DATETIME          //9
		// Fix Later            //SQL_VARCHAR           //12
		// Fix Later            //SQL_TYPE_DATE         //91
		// Fix Later            //SQL_TYPE_TIME         //92
		// Fix Later            //SQL_TYPE_TIMESTAMP    //93
		SQLBIGINT SqlBigInt; // ODBCINT64
		} data;
	};

	struct ColumnInfo
	{
		int				Index;
#ifdef UNICODE
		SQLWCHAR			ColumnName[32];
#else
		SQLCHAR			ColumnName[32];
#endif
		SQLSMALLINT		ColumnNameLength;
		SQLSMALLINT		ColumnType;
		SQLULEN 		ColumnSize;
		SQLSMALLINT		DecimalDigits;
		SQLSMALLINT		Nullable;
		SQLPOINTER		TargetValue;
		double			Max;
		double			Min;
		double			m;
		double			b;
	};

	//struct PeriodKey
	//{
	//	std::vector<SQLINTEGER> Key;
	//};

	//struct PeriodKeyCmp
	//{
	//	bool operator()(const PeriodKey& lhs, const PeriodKey& rhs) const;
	//};

	//struct IndexKey
	//{
	//	SQLINTEGER StockPriceId;
	//};

	//struct IndexKeyCmp
	//{
	//	bool operator()(const IndexKey& lhs, const IndexKey& rhs) const;
	//};

	typedef std::map< std::string, ColumnInfo > ColInfoType;
	typedef std::map< std::string, Field > Record;
	typedef std::vector< Record > TableData;
//	typedef std::map< PeriodKey, Record, PeriodKeyCmp > PeriodKeyToRecordMapType;
	typedef std::map< std::string, Record > PeriodKeyToRecordMapType;
//	typedef std::map< IndexKey, PeriodKeyToRecordMapType, IndexKeyCmp > TableData_IX;
	typedef std::map< SQLINTEGER, PeriodKeyToRecordMapType > TableData_IX;
	typedef std::vector< std::string > KeyList;

	SQLHSTMT SQLPrepare( char* StatementText );
	SQLRETURN SQLBindUnsignedLongInputParameter( SQLHSTMT hstmt, int ParameterNumber, unsigned long* ParameterValue );
	SQLRETURN SQLBindUnsignedIntegerInputParameter( SQLHSTMT hstmt, int ParameterNumber, unsigned int* ParameterValue );
	SQLRETURN SQLBindDoubleInputParameter( SQLHSTMT hstmt, int ParameterNumber, double* ParameterValue );
	SQLRETURN SQLBindCharInputParameter( SQLHSTMT hstmt, int ParameterNumber, const char* ParameterValue );
//	SQLRETURN SQLBindInputParameter( SQLHSTMT hstmt, int ParameterNumber, const std::string ParameterValue );

//	SQLRETURN SQLBindOutputParameter( int _parameterNumber, unsigned long* _parameterValue);

	SQLRETURN SQLExecute( SQLHSTMT hstmt );
	//SQLRETURN SQLExecDirect( char* StatementText );
	//SQLRETURN SQLExecDirectAndFetch( char* StatementText );

	unsigned long GetRowCount( char* tableName );
#ifdef UNICODE
	unsigned long GetSlowRowCount(const std::wstring& tableName);
#else
	unsigned long GetSlowRowCount( const std::string& tableName );
#endif
	void GetTable( ColInfoType& _colInfo, TableData& _tableData, const std::string& _tableName, const std::string& _whereClause = 0 );
	void GetTable( ColInfoType& _colInfo, TableData_IX& _tableData, const std::string& _tableName, const KeyList& _keyList, const std::string& _whereClause );
//	void GetTable( ColInfoType& _colInfo, TableData_IX& _tableData, const std::string& _tableName, const std::string& _orderByClause, int n, ... );

private:

	SQLHENV henv;
	SQLHDBC hdbc;
	SQLRETURN retcode;

#ifdef UNICODE
	SQLWCHAR		V_OD_stat[10]; // Status SQL
	SQLWCHAR		V_OD_msg[2000];
#else
	SQLCHAR			V_OD_stat[10]; // Status SQL
	SQLCHAR			V_OD_msg[2000];
#endif
	SQLSMALLINT		 V_OD_mlen;
	SQLINTEGER		 V_OD_err;

#ifdef UNICODE
	void getTable(ColInfoType& _colInfo, TableData& _tableData, TableData_IX& _tableData_ix, const std::wstring& _tableName, const std::wstring& _whereClause, bool useIndex, const KeyList& _keyList);
#else
	void getTable( ColInfoType& _colInfo, TableData& _tableData, TableData_IX& _tableData_ix, const std::string& _tableName, const std::string& _whereClause, bool useIndex, const KeyList& _keyList );
#endif
};

//extern SqlConnection sqlConnection;

typedef Singleton<SqlConnection> SqlConnectionSingleton;   // Global declaration

} // namespace mypushgp
