#ifndef _STOCKPRICE__H__
#define _STOCKPRICE__H__

#include <boost/serialization/vector.hpp>

#include "..\Database\SqlConnection.h"
#include <iostream>
//#include <string>
//#include <vector>
//#include <sql.h>
//#include <sqlext.h>
//#include <sqltypes.h>

namespace domain
{
	// Object to represent a Date & Time
	class DateTime
	{
	public:
		DateTime() : year_(0), month_(0), day_(0), hour_(0), minute_(0), second_(0) {};
		DateTime(const SQL_TIMESTAMP_STRUCT & _dateTime) : 
			year_(_dateTime.year), 
			month_(_dateTime.month), 
			day_(_dateTime.day), 
			hour_(_dateTime.hour), 
			minute_(_dateTime.minute), 
			second_(_dateTime.second)
			{} ;

		unsigned int Year()   const { return year_; }
		unsigned int Month()  const { return month_; }
		unsigned int Day()    const { return day_; }
		unsigned int Hour()   const { return hour_; }
		unsigned int Minute() const { return minute_; }
		unsigned int Second() const { return second_; }

		unsigned long Value();

	private:
		friend class boost::serialization::access;

		unsigned int year_;
		unsigned int month_;
		unsigned int day_;
		unsigned int hour_;
		unsigned int minute_;
		unsigned int second_;

		template<class Archive>
		void serialize(Archive & ar, const unsigned int version)
		{
			ar & year_;
			ar & month_;
			ar & day_;
			ar & hour_;
			ar & minute_;
			ar & second_;
		}
	};

	class StockPrice
	{
	public:
		unsigned int Id()      const { return id_; }
		DateTime     Date()    const { return date_; }
		double       AdjOpen() const { return adjOpen_; }

		StockPrice() : id_(0), adjOpen_(0.0) {};

		StockPrice(
			unsigned int _id,
			const SQL_TIMESTAMP_STRUCT& _date,
			unsigned int _month,
			unsigned int _day,
			unsigned int _dayOfYear,
			unsigned int _week,
			unsigned int _weekDay, 
			double _adjOpen,
			double _normalizedOpen,
			double _normalizedClose,
			double _normalizedHigh,
			double _normalizedLow,
			double _normalizedVolume);

		int AddData( const double _data, const std::string _label);
		int AddData(const double _data);
		double GetData( int column );
		int GetNumberOfDataColumns() { return data_record_.size(); };
		void normalizeData( const int _index, const double _m, const double _b );

	private:
		friend class boost::serialization::access;

		static const std::string columnNamesFilename_;

		unsigned int id_;
		DateTime date_;
		double adjOpen_;
		std::vector<double> data_record_;

		template<class Archive>
		void serialize(Archive & ar, const unsigned int version)
		{
			ar & id_;
			ar & date_;
			ar & adjOpen_;
			ar & data_record_;
		}
	};
}

#endif
