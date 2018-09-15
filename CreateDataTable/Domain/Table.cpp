#define __STDC_WANT_LIB_EXT1__ 1

#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <locale.h>

#include <iostream>
#include <fstream>

#include <boost/archive/binary_oarchive.hpp> // saving
#include <boost/archive/binary_iarchive.hpp> // loading
#include <initializer_list>
//#include <algorithm>    // std::lower_bound, std::upper_bound, std::sort

#include "Table.h"
#include "..\Utilities\MyException.h"
#include "..\Parameters.h"

#undef max;
#undef min;

namespace domain
{
std::vector<StockPrice> Table::trainingStockPrices_ = std::vector<StockPrice>();
Table::ColumnInfoMapType Table::columnInfoMap_ = Table::ColumnInfoMapType();

int Table::GetNumberOfTrainingDataRows()
{ 
	return trainingStockPrices_.size(); 
}


/* Constructors */
Table::Table()
{
	loadStockPrices();
}

Table::~Table()
{
}


/* Methods */
StockPrice Table::GetStockPrice(unsigned int index)
{
	try
	{
		return trainingStockPrices_[index];
	}

	catch (MyException& e)
	{
		std::cerr << "MyException caught" << std::endl;
		std::cerr << e.what() << std::endl;
		std::cerr << "index = " << index << std::endl;

		throw (e);
	}
}

std::string Table::GetColumnName( int _column )
{
	std::string colName;
	int	column = (_column < 0) ? (-1 - _column) : (_column);

	for ( ColumnInfoMapType::iterator it = columnInfoMap_.begin() ; it != columnInfoMap_.end(); ++it)
	{
		if (column == it->second.index)
			colName = it->first;
	}

	return colName;
}

unsigned int Table::GetIndicator(unsigned int _indicatorId, unsigned int _dataRowIndex)
{
	static unsigned int state = 0;
	unsigned int columnNumber = 0;

	switch (_indicatorId)
	{
		case 1:
			columnNumber = 1311;	// CCI Correction 300
			break;

		case 2:
			columnNumber = 1259;	// CCI Correction 180

		case 3:
			columnNumber = 1339;	// CCI Correction 90
			break;

		case 4:
			columnNumber = 1323;	// CCI Correction 60
			break;

		case 5:
			columnNumber = 1307;	// CCI Correction 30
			break;
	}

	double CCI = GetStockData(_dataRowIndex, columnNumber);

	if (CCI >= 100.0)
		state = 1;

	else if (CCI <= -100.0)
		state = 0;

	return state;
}

Table::DataRange Table::GetDataRange(const unsigned long from, const unsigned long to)
{
	DataRange dataRange;

	dataRange.from = BinarySearchRight(from);
	dataRange.to = BinarySearchLeft(to);

	return dataRange;
}

unsigned long Table::BinarySearchLeft(const unsigned long target)
{
	std::vector<StockPrice>::size_type left = 0;
	std::vector<StockPrice>::size_type right = trainingStockPrices_.size();
	std::vector<StockPrice>::size_type middle = 0;

	while (left < right)
	{
		middle = ((left + right) / 2);

		if (trainingStockPrices_[middle].Date().Value() < target)
			left = ++middle;
		else
			right = middle;
	}

	if (trainingStockPrices_[middle].Date().Value() == target)
		return left;

	else
		return --left;
}

unsigned long Table::BinarySearchRight(const unsigned long target)
{
	std::vector<StockPrice>::size_type left = 0;
	std::vector<StockPrice>::size_type right = trainingStockPrices_.size();
	std::vector<StockPrice>::size_type middle = 0;

	while (left < right)
	{
		middle = ((left + right) / 2);

		if (trainingStockPrices_[middle].Date().Value() > target)
			right = middle;
		else
			left = ++middle;
	}

	if (trainingStockPrices_[middle].Date().Value() == target)
		return --left;

	else
		return left;
}

void Table::ExportDataTable(const char* pExportFileName, const unsigned long from, const unsigned long to)
{
	DataRange dataRange = GetDataRange(from, to);
	int n;

	std::ofstream out(pExportFileName);

	//	Write header row
	for (n = 0; n < trainingStockPrices_[0].GetNumberOfDataColumns() - 1; n++)
		out << std::to_string(n) << ",";

	out << std::to_string(n) << "\n";

	// Write data rows
	int last_column = trainingStockPrices_[0].GetNumberOfDataColumns() - 1;

	for (int row = dataRange.from; row < dataRange.to; row++)
	{
		for (int column = 0; column < last_column; column++)
			out << std::to_string(GetStockData(row, column)) << ",";

		out << std::to_string(GetStockData(row, last_column)) << "\n";
	}

	out.close();
}

/// <summary>Returns the data from the Stock Data Array pointed to by the row and column parameters</summary>
/// <param name="row">Zero-based Row number index.  Must be greater than zero for derivative</param>  
/// <param name="colum">Zero-based Column number index for stock data, negative column number for first derivative</param>  
/// <returns>Stock data if column is less than or equal to 0, first derivative of stock data if column less than 0</returns>
double Table::GetStockData( unsigned int row, int column )
{
	double value = 0.0;

	if (column < 0)
	{
		if (row > 0)
		{
			column = -1 - column;
			value = trainingStockPrices_[row].GetData(column) - trainingStockPrices_[row - 1].GetData(column);
		}
	}

	else
		value = trainingStockPrices_[ row ].GetData( column );

	double a = abs(value);

	if ((a < 0.00000001) || (a > 100000000.0))
		value = 0.0;

	return value;
}


/* Private */

// Attempts to load the Stock data from the file cache.  If the file cache does 
// not exists, then will load Stock data from StockPrices table in database 
// then create the file cache to improve performance for the next time the data 
// needs to be loaded.
void Table::loadStockPrices()
{
	std::string sqlStatement;
	std::vector<std::string> keyList;

	database::SqlConnection::ColInfoType colInfo;
	database::SqlConnection::TableData tableData;
	database::SqlConnection::TableData_IX tableData_IX;

	if (trainingStockPrices_.size() == 0)
	{

		if (loadStockDataFromArchive() == false)
		{

			// Load Stock Data
			sqlStatement =  "SELECT [Id],[Symbol],[Date],DATEPART ( m , [Date] ) AS [Month],DATEPART ( d , [Date] ) AS [Day],DATEPART ( dy , [Date] ) AS [DayOfYear],DATEPART ( WEEK , [Date] ) AS [Week],DATEPART ( WEEKDAY , [Date] ) AS [WeekDay],[Open],[High],[Low],[Close],[Volume],[Adj Close] ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;															//[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";

			database::SqlConnectionSingleton::Instance()->GetTable( colInfo, tableData, sqlStatement, "ORDER BY [Date]" );

			for (database::SqlConnection::TableData::iterator it = tableData.begin() ; it != tableData.end(); ++it)
			{
				database::SqlConnection::Record record = *it;

				double adjClose = record["Adj Close"].data.SqlFloat;
				double adjOpen = record[ "Adj Close" ].data.SqlFloat / record[ "Close" ].data.SqlFloat * record[ "Open" ].data.SqlFloat;
				double adjHigh = record["Adj Close"].data.SqlFloat / record["Close"].data.SqlFloat * record["High"].data.SqlFloat;
				double adjLow = record["Adj Close"].data.SqlFloat / record["Close"].data.SqlFloat * record["Low"].data.SqlFloat;
				double adjVolume = record["Close"].data.SqlFloat / record["Adj Close"].data.SqlFloat * record["Volume"].data.SqlBigInt;

				long id = record["Id"].data.SqlInteger;

				StockPrice sp = StockPrice
				(
					record["Id"].data.SqlInteger,
					record["Date"].data.SqlDateTime,
					record["Month"].data.SqlInteger,
					record["Day"].data.SqlInteger,
					record["DayOfYear"].data.SqlInteger,
					record["Week"].data.SqlInteger,
					record["WeekDay"].data.SqlInteger,
					adjOpen,
					adjOpen,  //record["Open"].data.SqlFloat,
					adjClose, //record["Close"].data.SqlFloat,
					adjHigh,  //record["High"].data.SqlFloat,
					adjLow,   //record["Low"].data.SqlFloat,
					adjVolume //record["Volume"].data.SqlBigInt
				);
				trainingStockPrices_.push_back(sp);
			}
			saveStockDataToArchive();


			sqlStatement =  "SELECT DISTINCT A.* ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "INNER JOIN [Decision Support System].[dbo].[Aggregate] A ";
			sqlStatement += "  ON S.Id = A.StockPriceId ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;															//[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";
			sqlStatement += "  AND  ";
			sqlStatement += "  ( ";
			sqlStatement += "    (Period >= 5 AND Period <= 30) ";
			sqlStatement += "		OR Period = 60 ";
			sqlStatement += "		OR Period = 90 ";
			sqlStatement += "		OR Period = 180 ";
			sqlStatement += "		OR Period = 300 ";
			
			sqlStatement += "	)";

			keyList.clear();
			keyList.push_back( "Period" );
			loadTechnicalData( "Aggregate", sqlStatement, keyList );

			saveStockDataToArchive();


			sqlStatement =  "SELECT DISTINCT A.* ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "INNER JOIN [Decision Support System].[dbo].[AcclDistLine] A ";
			sqlStatement += "  ON S.Id = A.StockPriceId ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;															//[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";

			keyList.clear();
			loadTechnicalData( "AcclDistLine", sqlStatement, keyList );

			saveStockDataToArchive();


			sqlStatement =  "SELECT DISTINCT A.* ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "INNER JOIN [Decision Support System].[dbo].[AcclDistLineAggregate] A ";
			sqlStatement += "  ON S.Id = A.StockPriceId ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;															//[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";
			sqlStatement += "  AND  ";
			sqlStatement += "  ( ";
			sqlStatement += "    (Period >= 5 AND Period <= 30) ";
			sqlStatement += "		OR Period = 60 ";
			sqlStatement += "		OR Period = 90 ";
			sqlStatement += "		OR Period = 180 ";
			sqlStatement += "		OR Period = 300 ";
			sqlStatement += "	)";

			keyList.clear();
			keyList.push_back( "Period" );
			loadTechnicalData( "AcclDistLineAggregate", sqlStatement, keyList );

			saveStockDataToArchive();


			sqlStatement =  "SELECT DISTINCT A.* ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "INNER JOIN [Decision Support System].[dbo].[AcclDistLineOBV] A ";
			sqlStatement += "  ON S.Id = A.StockPriceId ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;															//[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";

			keyList.clear();
			loadTechnicalData( "AcclDistLineOBV", sqlStatement, keyList );

			saveStockDataToArchive();


			sqlStatement =  "SELECT DISTINCT A.* ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "INNER JOIN [Decision Support System].[dbo].[ADX] A ";
			sqlStatement += "  ON S.Id = A.StockPriceId ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;															//[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";

			keyList.clear();
			keyList.push_back( "Period" );
			keyList.push_back( "TrendLevel" );
			loadTechnicalData( "ADX", sqlStatement, keyList );

			saveStockDataToArchive();


			sqlStatement =  "SELECT DISTINCT A.* ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "INNER JOIN [Decision Support System].[dbo].[Aroon] A ";
			sqlStatement += "  ON S.Id = A.StockPriceId ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";	
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;															//[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";
			sqlStatement += "  AND  ";
			sqlStatement += "  ( ";
			sqlStatement += "    (Period >= 5 AND Period <= 30) ";
			sqlStatement += "		OR Period = 60 ";
			sqlStatement += "		OR Period = 90 ";
			sqlStatement += "		OR Period = 180 ";
			sqlStatement += "		OR Period = 300 ";
			sqlStatement += "	)";

			keyList.clear();
			keyList.push_back( "Period" );
			loadTechnicalData( "Aroon", sqlStatement, keyList );

			saveStockDataToArchive();


			sqlStatement =  "SELECT DISTINCT A.* ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "INNER JOIN [Decision Support System].[dbo].[ATR] A ";
			sqlStatement += "  ON S.Id = A.StockPriceId ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;															//[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";
			sqlStatement += "  AND  ";
			sqlStatement += "  ( ";
			sqlStatement += "    (Period >= 5 AND Period <= 30) ";
			sqlStatement += "		OR Period = 60 ";
			sqlStatement += "		OR Period = 90 ";
			sqlStatement += "		OR Period = 180 ";
			sqlStatement += "		OR Period = 300 ";
			sqlStatement += "	)";

			keyList.clear();
			keyList.push_back( "Period" );
			loadTechnicalData( "ATR", sqlStatement, keyList );

			saveStockDataToArchive();


			sqlStatement =  "SELECT DISTINCT A.* ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "INNER JOIN [Decision Support System].[dbo].[BB] A ";
			sqlStatement += "  ON S.Id = A.StockPriceId ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;															//[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";
			sqlStatement += "  AND  ";
			sqlStatement += "  ( ";
			sqlStatement += "    (Period >= 5 AND Period <= 30) ";
			sqlStatement += "		OR Period = 60 ";
			sqlStatement += "		OR Period = 90 ";
			sqlStatement += "		OR Period = 180 ";
			sqlStatement += "		OR Period = 300 ";
			sqlStatement += "	)";

			keyList.clear();
			keyList.push_back( "Period" );
			loadTechnicalData( "BB", sqlStatement, keyList );
	
			saveStockDataToArchive();

			sqlStatement =  "SELECT DISTINCT A.* ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "INNER JOIN [Decision Support System].[dbo].[CCI] A ";
			sqlStatement += "  ON S.Id = A.StockPriceId ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;															//[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";
			sqlStatement += "  AND  ";
			sqlStatement += "  ( ";
			sqlStatement += "    (Period >= 5 AND Period <= 30) ";
			sqlStatement += "		OR Period = 60 ";
			sqlStatement += "		OR Period = 90 ";
			sqlStatement += "		OR Period = 180 ";
			sqlStatement += "		OR Period = 300 ";
			sqlStatement += "	)";

			keyList.clear();
			keyList.push_back( "Period" );
			loadTechnicalData( "CCI", sqlStatement, keyList );
	
			saveStockDataToArchive();


			sqlStatement =  "SELECT DISTINCT A.* ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "INNER JOIN [Decision Support System].[dbo].[ChaikinOscillator] A ";
			sqlStatement += "  ON S.Id = A.StockPriceId ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;															//[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";

			keyList.clear();
			keyList.push_back( "FastPeriod" );
			keyList.push_back( "SlowPeriod" );
			loadTechnicalData( "ChaikinOscillator", sqlStatement, keyList );
	
			saveStockDataToArchive();


			sqlStatement =  "SELECT DISTINCT A.* ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "INNER JOIN [Decision Support System].[dbo].[Chandelier] A ";
			sqlStatement += "  ON S.Id = A.StockPriceId ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;															//[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";
			sqlStatement += "  AND  ";
			sqlStatement += "  ( ";
			sqlStatement += "    (Period >= 5 AND Period <= 30) ";
			sqlStatement += "		OR Period = 60 ";
			sqlStatement += "		OR Period = 90 ";
			sqlStatement += "		OR Period = 180 ";
			sqlStatement += "		OR Period = 300 ";
			sqlStatement += "	)";

			keyList.clear();
			keyList.push_back( "Period" );
			loadTechnicalData( "Chandelier", sqlStatement, keyList );
	
			saveStockDataToArchive();


			sqlStatement =  "SELECT DISTINCT A.* ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "INNER JOIN [Decision Support System].[dbo].[CMF] A ";
			sqlStatement += "  ON S.Id = A.StockPriceId ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;															//[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";
			sqlStatement += "  AND  ";
			sqlStatement += "  ( ";
			sqlStatement += "    (Period >= 5 AND Period <= 30) ";
			sqlStatement += "		OR Period = 60 ";
			sqlStatement += "		OR Period = 90 ";
			sqlStatement += "		OR Period = 180 ";
			sqlStatement += "		OR Period = 300 ";
			sqlStatement += "	)";

			keyList.clear();
			keyList.push_back( "Period" );
			loadTechnicalData( "CMF", sqlStatement, keyList );

			saveStockDataToArchive();


			sqlStatement =  "SELECT DISTINCT A.* ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "INNER JOIN [Decision Support System].[dbo].[CoppockCurve] A ";
			sqlStatement += "  ON S.Id = A.StockPriceId ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;															//[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";

			keyList.clear();
			keyList.push_back( "Period" );
			keyList.push_back( "FastPeriod" );
			keyList.push_back( "SlowPeriod" );
			loadTechnicalData( "CoppockCurve", sqlStatement, keyList );
	
			saveStockDataToArchive();


			sqlStatement =  "SELECT DISTINCT A.* ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "INNER JOIN [Decision Support System].[dbo].[DPO] A ";
			sqlStatement += "  ON S.Id = A.StockPriceId ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;															//[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";
			sqlStatement += "  AND  ";
			sqlStatement += "  ( ";
			sqlStatement += "    (Period >= 5 AND Period <= 30) ";
			sqlStatement += "		OR Period = 60 ";
			sqlStatement += "		OR Period = 90 ";
			sqlStatement += "		OR Period = 180 ";
			sqlStatement += "		OR Period = 300 ";
			sqlStatement += "	)";

			keyList.clear();
			keyList.push_back( "Period" );
			loadTechnicalData( "DPO", sqlStatement, keyList );
	
			saveStockDataToArchive();


			sqlStatement =  "SELECT DISTINCT A.* ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "INNER JOIN [Decision Support System].[dbo].[EMV] A ";
			sqlStatement += "  ON S.Id = A.StockPriceId ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;															//[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";
			sqlStatement += "  AND  ";
			sqlStatement += "  ( ";
			sqlStatement += "    (Period >= 5 AND Period <= 30) ";
			sqlStatement += "		OR Period = 60 ";
			sqlStatement += "		OR Period = 90 ";
			sqlStatement += "		OR Period = 180 ";
			sqlStatement += "		OR Period = 300 ";
			sqlStatement += "	)";

			keyList.clear();
			keyList.push_back( "Period" );
			loadTechnicalData( "EMV", sqlStatement, keyList );
	
			saveStockDataToArchive();


			sqlStatement =  "SELECT DISTINCT A.* ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "INNER JOIN [Decision Support System].[dbo].[ForceIndex] A ";
			sqlStatement += "  ON S.Id = A.StockPriceId ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;															//[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";
			sqlStatement += "  AND  ";
			sqlStatement += "  ( ";
			sqlStatement += "    (Period >= 5 AND Period <= 30) ";
			sqlStatement += "		OR Period = 60 ";
			sqlStatement += "		OR Period = 90 ";
			sqlStatement += "		OR Period = 180 ";
			sqlStatement += "		OR Period = 300 ";
			sqlStatement += "	)";

			keyList.clear();
			keyList.push_back( "Period" );
			loadTechnicalData( "ForceIndex", sqlStatement, keyList );
	
			saveStockDataToArchive();


			sqlStatement =  "SELECT DISTINCT A.* ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "INNER JOIN [Decision Support System].[dbo].[ForceIndexTrend] A ";
			sqlStatement += "  ON S.Id = A.StockPriceId ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;															//[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";

			keyList.clear();
			keyList.push_back( "Period" );
			loadTechnicalData( "ForceIndexTrend", sqlStatement, keyList );
	
			saveStockDataToArchive();


			sqlStatement =  "SELECT DISTINCT A.* ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "INNER JOIN [Decision Support System].[dbo].[Hurst] A ";
			sqlStatement += "  ON S.Id = A.StockPriceId ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;															//[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";

			keyList.clear();
			keyList.push_back( "Period" );
			loadTechnicalData( "Hurst", sqlStatement, keyList );
	
			saveStockDataToArchive();


			sqlStatement =  "SELECT DISTINCT A.* ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "INNER JOIN [Decision Support System].[dbo].[IchimokuCloud] A ";
			sqlStatement += "  ON S.Id = A.StockPriceId ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;															//[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";

			keyList.clear();
			keyList.push_back( "ConversionLinePeriod" );
			keyList.push_back( "BaseLinePeriod" );
			keyList.push_back( "LeadingSpanPeriod" );
			keyList.push_back( "LaggingSpanPeriod" );
			loadTechnicalData( "IchimokuCloud", sqlStatement, keyList );
	
			saveStockDataToArchive();


			sqlStatement =  "SELECT DISTINCT A.* ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "INNER JOIN [Decision Support System].[dbo].[KeltnerChannels] A ";
			sqlStatement += "  ON S.Id = A.StockPriceId ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;															//[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";

			keyList.clear();
			keyList.push_back( "EMAPeriod" );
			keyList.push_back( "ATRMultiplier" );
			keyList.push_back( "ATRPeriod" );
			loadTechnicalData( "KeltnerChannels", sqlStatement, keyList );
	
			saveStockDataToArchive();


			sqlStatement =  "SELECT DISTINCT A.* ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "INNER JOIN [Decision Support System].[dbo].[KSI] A ";
			sqlStatement += "  ON S.Id = A.StockPriceId ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;															//[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";

			keyList.clear();
			keyList.push_back( "ROC1Period" );
			keyList.push_back( "ROC2Period" );
			keyList.push_back( "ROC3Period" );
			keyList.push_back( "ROC4Period" );
			keyList.push_back( "SMAROC1Period" );
			keyList.push_back( "SMAROC2Period" );
			keyList.push_back( "SMAROC3Period" );
			keyList.push_back( "SMAROC4Period" );
			keyList.push_back( "SignalLineMovingAveragePeriod" );
			loadTechnicalData( "KSI", sqlStatement, keyList );
	
			saveStockDataToArchive();


			sqlStatement =  "SELECT DISTINCT A.* ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "INNER JOIN [Decision Support System].[dbo].[MACD] A ";
			sqlStatement += "  ON S.Id = A.StockPriceId ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;															//[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";

			keyList.clear();
			keyList.push_back( "FastPeriod" );
			keyList.push_back( "SlowPeriod" );
			keyList.push_back( "SignalLinePeriod" );
			loadTechnicalData( "MACD", sqlStatement, keyList );
	
			saveStockDataToArchive();


			sqlStatement =  "SELECT DISTINCT A.* ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "INNER JOIN [Decision Support System].[dbo].[MassIndex] A ";
			sqlStatement += "  ON S.Id = A.StockPriceId ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;															//[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";

			keyList.clear();
			keyList.push_back( "Period" );
			loadTechnicalData( "MassIndex", sqlStatement, keyList );
	
			saveStockDataToArchive();


			sqlStatement =  "SELECT DISTINCT A.* ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "INNER JOIN [Decision Support System].[dbo].[MFI] A ";
			sqlStatement += "  ON S.Id = A.StockPriceId ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;															//[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";
			sqlStatement += "  AND  ";
			sqlStatement += "  ( ";
			sqlStatement += "    (Period >= 5 AND Period <= 30) ";
			sqlStatement += "		OR Period = 60 ";
			sqlStatement += "		OR Period = 90 ";
			sqlStatement += "		OR Period = 180 ";
			sqlStatement += "		OR Period = 300 ";
			sqlStatement += "	)";

			keyList.clear();
			keyList.push_back( "Period" );
			loadTechnicalData( "MFI", sqlStatement, keyList );

			saveStockDataToArchive();


			sqlStatement =  "SELECT DISTINCT A.* ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "INNER JOIN [Decision Support System].[dbo].[NVI] A ";
			sqlStatement += "  ON S.Id = A.StockPriceId ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;															//[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";

			keyList.clear();
			keyList.push_back( "Period" );
			loadTechnicalData( "NVI", sqlStatement, keyList );
	
			saveStockDataToArchive();


			sqlStatement =  "SELECT DISTINCT A.* ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "INNER JOIN [Decision Support System].[dbo].[OBV] A ";
			sqlStatement += "  ON S.Id = A.StockPriceId ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;															//[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";

			keyList.clear();
			loadTechnicalData( "OBV", sqlStatement, keyList );
	
			saveStockDataToArchive();


			sqlStatement =  "SELECT DISTINCT A.* ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "INNER JOIN [Decision Support System].[dbo].[OBVA] A ";
			sqlStatement += "  ON S.Id = A.StockPriceId ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;															//[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";
			sqlStatement += "  AND  ";
			sqlStatement += "  ( ";
			sqlStatement += "    (Period >= 5 AND Period <= 30) ";
			sqlStatement += "		OR Period = 60 ";
			sqlStatement += "		OR Period = 90 ";
			sqlStatement += "		OR Period = 180 ";
			sqlStatement += "		OR Period = 300 ";
			sqlStatement += "	)";

			keyList.clear();
			keyList.push_back( "Period" );
			loadTechnicalData( "OBVA", sqlStatement, keyList );

			saveStockDataToArchive();


			sqlStatement =  "SELECT DISTINCT A.* ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "INNER JOIN [Decision Support System].[dbo].[ParabolicSAR] A ";
			sqlStatement += "  ON S.Id = A.StockPriceId ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;															//[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";

			keyList.clear();
			keyList.push_back( "Step" );
			keyList.push_back( "MaximumStep" );
			loadTechnicalData( "ParabolicSAR", sqlStatement, keyList );
	
			saveStockDataToArchive();


			sqlStatement =  "SELECT DISTINCT A.* ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "INNER JOIN [Decision Support System].[dbo].[PivotsPoints] A ";
			sqlStatement += "  ON S.Id = A.StockPriceId ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;               //[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";

			keyList.clear();
			loadTechnicalData( "PivotsPoint", sqlStatement, keyList );
	
			saveStockDataToArchive();


			sqlStatement =  "SELECT DISTINCT A.* ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "INNER JOIN [Decision Support System].[dbo].[PPO] A ";
			sqlStatement += "  ON S.Id = A.StockPriceId ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;               //[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";

			keyList.clear();
			keyList.push_back( "FastPeriod" );
			keyList.push_back( "SlowPeriod" );
			keyList.push_back( "SignalLinePeriod" );
			loadTechnicalData( "PPO", sqlStatement, keyList );
	
			saveStockDataToArchive();


			sqlStatement =  "SELECT DISTINCT A.* ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "INNER JOIN [Decision Support System].[dbo].[PriceChannels] A ";
			sqlStatement += "  ON S.Id = A.StockPriceId ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;               //[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";
			sqlStatement += "  AND  ";
			sqlStatement += "  ( ";
			sqlStatement += "    (Period >= 5 AND Period <= 30) ";
			sqlStatement += "		OR Period = 60 ";
			sqlStatement += "		OR Period = 90 ";
			sqlStatement += "		OR Period = 180 ";
			sqlStatement += "		OR Period = 300 ";
			sqlStatement += "	)";

			keyList.clear();
			keyList.push_back( "Period" );
			loadTechnicalData( "PriceChannels", sqlStatement, keyList );
	
			saveStockDataToArchive();


			sqlStatement =  "SELECT DISTINCT A.* ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "INNER JOIN [Decision Support System].[dbo].[PVO] A ";
			sqlStatement += "  ON S.Id = A.StockPriceId ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;               //[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";

			keyList.clear();
			keyList.push_back( "FastPeriod" );
			keyList.push_back( "SlowPeriod" );
			keyList.push_back( "SignalLinePeriod" );
			loadTechnicalData( "PVO", sqlStatement, keyList );
	
			saveStockDataToArchive();


			sqlStatement =  "SELECT DISTINCT A.* ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "INNER JOIN [Decision Support System].[dbo].[RelativeDate] A ";
			sqlStatement += "  ON S.Id = A.StockPriceId ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;               //[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";

			keyList.clear();
			loadTechnicalData( "RelativeDate", sqlStatement, keyList );
	
			saveStockDataToArchive();


			sqlStatement =  "SELECT DISTINCT A.* ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "INNER JOIN [Decision Support System].[dbo].[RSI] A ";
			sqlStatement += "  ON S.Id = A.StockPriceId ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;               //[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";
			sqlStatement += "  AND  ";
			sqlStatement += "  ( ";
			sqlStatement += "    (Period >= 5 AND Period <= 30) ";
			sqlStatement += "		OR Period = 60 ";
			sqlStatement += "		OR Period = 90 ";
			sqlStatement += "		OR Period = 180 ";
			sqlStatement += "		OR Period = 300 ";
			sqlStatement += "	)";

			keyList.clear();
			keyList.push_back( "Period" );
			loadTechnicalData( "RSI", sqlStatement, keyList );
	
			saveStockDataToArchive();


			sqlStatement =  "SELECT DISTINCT A.* ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "INNER JOIN [Decision Support System].[dbo].[SCTR] A ";
			sqlStatement += "  ON S.Id = A.StockPriceId ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;               //[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";

			keyList.clear();
			loadTechnicalData( "SCTR", sqlStatement, keyList );
	
			saveStockDataToArchive();


			sqlStatement =  "SELECT DISTINCT A.* ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "INNER JOIN [Decision Support System].[dbo].[Slope] A ";
			sqlStatement += "  ON S.Id = A.StockPriceId ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;               //[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";
			sqlStatement += "  AND  ";
			sqlStatement += "  ( ";
			sqlStatement += "    (Period >= 5 AND Period <= 30) ";
			sqlStatement += "		OR Period = 60 ";
			sqlStatement += "		OR Period = 90 ";
			sqlStatement += "		OR Period = 180 ";
			sqlStatement += "		OR Period = 300 ";
			sqlStatement += "	)";

			keyList.clear();
			keyList.push_back( "Period" );
			loadTechnicalData( "Slope", sqlStatement, keyList );
	
			saveStockDataToArchive();


			sqlStatement =  "SELECT DISTINCT A.* ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "INNER JOIN [Decision Support System].[dbo].[StandardDeviation] A ";
			sqlStatement += "  ON S.Id = A.StockPriceId ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;               //[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";
			sqlStatement += "  AND  ";
			sqlStatement += "  ( ";
			sqlStatement += "    (Period >= 5 AND Period <= 30) ";
			sqlStatement += "		OR Period = 60 ";
			sqlStatement += "		OR Period = 90 ";
			sqlStatement += "		OR Period = 180 ";
			sqlStatement += "		OR Period = 300 ";
			sqlStatement += "	)";

			keyList.clear();
			keyList.push_back( "Period" );
			loadTechnicalData( "StandardDeviation", sqlStatement, keyList );
	
			saveStockDataToArchive();


			sqlStatement =  "SELECT DISTINCT A.* ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "INNER JOIN [Decision Support System].[dbo].[Stoch] A ";
			sqlStatement += "  ON S.Id = A.StockPriceId ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;               //[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";
			sqlStatement += "  AND  ";
			sqlStatement += "  ( ";
			sqlStatement += "    (Period >= 5 AND Period <= 30) ";
			sqlStatement += "		OR Period = 60 ";
			sqlStatement += "		OR Period = 90 ";
			sqlStatement += "		OR Period = 180 ";
			sqlStatement += "		OR Period = 300 ";
			sqlStatement += "	)";

			keyList.clear();
			keyList.push_back( "Period" );
			loadTechnicalData( "Stoch", sqlStatement, keyList );
	
			saveStockDataToArchive();


			sqlStatement =  "SELECT DISTINCT A.* ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "INNER JOIN [Decision Support System].[dbo].[StochRSI] A ";
			sqlStatement += "  ON S.Id = A.StockPriceId ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;               //[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";
			sqlStatement += "  AND  ";
			sqlStatement += "  ( ";
			sqlStatement += "    (Period >= 5 AND Period <= 30) ";
			sqlStatement += "		OR Period = 60 ";
			sqlStatement += "		OR Period = 90 ";
			sqlStatement += "		OR Period = 180 ";
			sqlStatement += "		OR Period = 300 ";
			sqlStatement += "	)";

			keyList.clear();
			keyList.push_back( "Period" );
			loadTechnicalData( "StochRSI", sqlStatement, keyList );
	
			saveStockDataToArchive();


			sqlStatement =  "SELECT DISTINCT A.* ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "INNER JOIN [Decision Support System].[dbo].[Trend] A ";
			sqlStatement += "  ON S.Id = A.StockPriceId ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;               //[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";

			keyList.clear();
			loadTechnicalData( "Trend", sqlStatement, keyList );
	
			saveStockDataToArchive();


			sqlStatement =  "SELECT DISTINCT A.* ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "INNER JOIN [Decision Support System].[dbo].[Trix] A ";
			sqlStatement += "  ON S.Id = A.StockPriceId ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;               //[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";

			keyList.clear();
			keyList.push_back( "Period" );
			keyList.push_back( "SignalLinePeriod" );
			loadTechnicalData( "Trix", sqlStatement, keyList );
	
			saveStockDataToArchive();


			sqlStatement =  "SELECT DISTINCT A.* ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "INNER JOIN [Decision Support System].[dbo].[TSI] A ";
			sqlStatement += "  ON S.Id = A.StockPriceId ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;               //[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";

			keyList.clear();
			keyList.push_back( "FirstSmoothingPeriod" );
			keyList.push_back( "SecondSmoothingPeriod" );
			keyList.push_back( "SignalLinePeriod" );
			loadTechnicalData( "TSI", sqlStatement, keyList );
	
			saveStockDataToArchive();


			sqlStatement =  "SELECT DISTINCT A.* ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "INNER JOIN [Decision Support System].[dbo].[Ulcer] A ";
			sqlStatement += "  ON S.Id = A.StockPriceId ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;               //[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";
			sqlStatement += "  AND  ";
			sqlStatement += "  ( ";
			sqlStatement += "    (Period >= 5 AND Period <= 30) ";
			sqlStatement += "		OR Period = 60 ";
			sqlStatement += "		OR Period = 90 ";
			sqlStatement += "		OR Period = 180 ";
			sqlStatement += "		OR Period = 300 ";
			sqlStatement += "	)";

			keyList.clear();
			keyList.push_back( "Period" );
			loadTechnicalData( "Ulcer", sqlStatement, keyList );
	
			saveStockDataToArchive();


			sqlStatement =  "SELECT DISTINCT A.* ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "INNER JOIN [Decision Support System].[dbo].[UO] A ";
			sqlStatement += "  ON S.Id = A.StockPriceId ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;               //[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";

			keyList.clear();
			keyList.push_back( "ShortAveragePeriod" );
			keyList.push_back( "MediumAveragePeriod" );
			keyList.push_back( "LongAveragePeriod" );
			loadTechnicalData( "UO", sqlStatement, keyList );
	
			saveStockDataToArchive();


			sqlStatement =  "SELECT DISTINCT A.* ";
			sqlStatement += "FROM [Decision Support System].[dbo].[StockPrices] S ";
			sqlStatement += "INNER JOIN [Decision Support System].[dbo].[VI] A ";
			sqlStatement += "  ON S.Id = A.StockPriceId ";
			sqlStatement += "WHERE [Symbol] = 'RIMM' ";
			sqlStatement += "  AND " + parameter.sqlDateRangeFilter;               //[Date] BETWEEN '1/3/2000  12:00:00 AM' AND '01/01/2015  12:00:00 AM' ";
			sqlStatement += "  AND  ";
			sqlStatement += "  ( ";
			sqlStatement += "    (Period >= 5 AND Period <= 30) ";
			sqlStatement += "		OR Period = 60 ";
			sqlStatement += "		OR Period = 90 ";
			sqlStatement += "		OR Period = 180 ";
			sqlStatement += "		OR Period = 300 ";
			sqlStatement += "	)";

			keyList.clear();
			keyList.push_back( "Period" );
			loadTechnicalData( "VI", sqlStatement, keyList );

			saveStockDataToArchive();
		} // if (loadStockDataFromArchive() == false)
	} // if (stockPrices.size() == 0)
}

void Table::loadTechnicalData( const std::string& _tableName, const std::string& _sqlStatement, std::vector< std::string > _keyList )
{
	int id;
	database::SqlConnection::ColInfoType colInfo;
	database::SqlConnection::TableData_IX tableData_IX;

	std::string orderByClause = "ORDER BY [StockPriceId]";

	if (_keyList.size() > 0)
	{
		for (std::vector< std::string >::iterator it = _keyList.begin(); it != _keyList.end(); ++it)
		{
			orderByClause += ", [" + *it + "]";
		}
	}

	database::SqlConnectionSingleton::Instance()->GetTable( colInfo, tableData_IX, _sqlStatement, _keyList, orderByClause );

	for ( std::vector<StockPrice>::iterator itStockPrice = trainingStockPrices_.begin(); itStockPrice != trainingStockPrices_.end(); ++itStockPrice)
	{
		id = itStockPrice->Id();

		database::SqlConnection::PeriodKeyToRecordMapType periodKeyMap = tableData_IX[ id ];

		for (database::SqlConnection::PeriodKeyToRecordMapType::iterator itPeriodKey = periodKeyMap.begin() ; itPeriodKey != periodKeyMap.end(); ++itPeriodKey)
		{
			for ( database::SqlConnection::ColInfoType::iterator itColumn = colInfo.begin() ; itColumn != colInfo.end(); ++itColumn)
			{
				std::string colName = itColumn->first;

				if ( (colName != "Id") && (colName != "StockPriceId") && (colName != "Period"))
				{
					double val;
					std::stringstream errorMessage;

					std::string columnInfoKey =  _tableName + " ";

					//for (int n = 0; n < (*itPeriodKey).first.Key.size(); n++)
					//	columnInfoKey += (*itPeriodKey).first.Key[n] + " ";
					columnInfoKey += (*itPeriodKey).first;

					columnInfoKey += colName;

//					struct _ColumnInfoKey columnInfoKey = { _tableName, (*itPeriodKey).first.Key, colName };
					struct _ColumnInfo columnInfo;

					if (columnInfoMap_.find( columnInfoKey ) == columnInfoMap_.end())
					{
						columnInfo.min = std::numeric_limits<double>::max();
						columnInfo.max = std::numeric_limits<double>::min();
					}

					else
						columnInfo = columnInfoMap_[ columnInfoKey ];

					switch ((*itPeriodKey).second[ colName ].type)
					{
						case SQL_CHAR: 
							errorMessage << "Error Table::loadTechnicalData() Unsupported type( SQL_CHAR ) " << std::endl;
							throw MyException(errorMessage);

							break;

						case SQL_INTEGER: 
							val = (*itPeriodKey).second[ colName ].data.SqlInteger;
							break;

						case SQL_FLOAT: 
							val = (*itPeriodKey).second[ colName ].data.SqlFloat;
							break;

						case SQL_TYPE_TIMESTAMP: 
							errorMessage << "Error Table::loadTechnicalData( " << _tableName << " ) Unsupported type( SQL_TYPE_TIMESTAMP ) " << std::endl;
							throw MyException(errorMessage);
							break;

						case SQL_BIGINT: //-5: // SQL_TYPE_BIGINT:
							val = (*itPeriodKey).second[ colName ].data.SqlBigInt;
							break;

						case SQL_BIT: //-7
							val = (*itPeriodKey).second[ colName ].data.SqlBit == 0 ? -1.0 : 1.0;
							break;

						default:
							errorMessage << "Error Table::loadTechnicalData( " << _tableName << " ) Unknown type( " << (*itPeriodKey).second[ colName ].type << " ) " << std::endl;
							throw MyException(errorMessage);

							break;
					}

					if (itStockPrice == trainingStockPrices_.begin())
						columnInfo.index = itStockPrice->AddData( val, columnInfoKey );
					else
						columnInfo.index = itStockPrice->AddData(val);

					columnInfo.min = columnInfo.min < val ? columnInfo.min : val;
					columnInfo.max = columnInfo.max > val ? columnInfo.max : val;

					columnInfoMap_[ columnInfoKey ] = columnInfo;
				}
			} // for ( database::SqlConnection::ColInfoType::iterator itColumn = colInfo.begin() ; itColumn != colInfo.end(); ++itColumn)
		} // for (int period = 5; period <= 300; period++)
	} // for ( std::vector<StockPrice>::iterator itStockPrice = stockPrices_.begin() ; itStockPrice != stockPrices_.end(); ++itStockPrice)

	for ( ColumnInfoMapType::iterator it = columnInfoMap_.begin() ; it != columnInfoMap_.end(); ++it)
	{
		std::string columnInfoKey = it->first;

		if (columnInfoMap_[ columnInfoKey ].min != columnInfoMap_[ columnInfoKey ].max)
		{
			columnInfoMap_[ columnInfoKey ].m = -2.0 / (columnInfoMap_[ columnInfoKey ].min - columnInfoMap_[ columnInfoKey ].max);
			columnInfoMap_[ columnInfoKey ].b = 1.0 - (columnInfoMap_[ columnInfoKey ].max * (-2.0 / (columnInfoMap_[ columnInfoKey ].min - columnInfoMap_[ columnInfoKey ].max)));
		}
	}

	//for ( std::vector<StockPrice>::iterator itStockPrice = stockPrices_.begin() ; itStockPrice != stockPrices_.end(); ++itStockPrice)
	//{
	//	for ( ColumnInfoMapType::iterator it = columnInfoMap_.begin() ; it != columnInfoMap_.end(); ++it)
	//	{
	//		std::string columnInfoKey = it->first;
	//				
	//		if (columnInfoMap_[ columnInfoKey ].min != columnInfoMap_[ columnInfoKey ].max)
	//			itStockPrice->normalizeData( columnInfoMap_[ columnInfoKey ].index, columnInfoMap_[ columnInfoKey ].m, columnInfoMap_[ columnInfoKey ].b );
	//	}
	//}
}

// Attempts to load the Stock data from the file cache.  If the file cache does 
// not exists, returns false.
bool Table::loadStockDataFromArchive()
{
	// open the archive
	std::ifstream infile(parameter.archiveFilename, std::ios::in | std::ios::binary);
	if (infile.is_open())
	{
		boost::archive::binary_iarchive ia(infile);

		// restore the schedule from the archive
		ia & (*this);
	
		return true;
	}

	else
		return false;
}

// Creates the file cache to improve performance for the next time the data 
// needs to be loaded.
bool Table::saveStockDataToArchive()
{
	std::ofstream ofs(parameter.archiveFilename, std::ios::out | std::ios::binary | std::ios::trunc);
	boost::archive::binary_oarchive oa(ofs);

	oa & (*this);

	return true;
}
} // namespace finance
