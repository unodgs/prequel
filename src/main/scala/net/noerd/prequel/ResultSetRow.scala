package net.noerd.prequel

import java.util.Date

import java.sql.ResultSet
import java.sql.ResultSetMetaData

import scala.collection.mutable.ArrayBuffer

import org.joda.time.DateTime
import org.joda.time.Duration

/**
 * Wraps a ResultSet in a row context. The ResultSetRow gives access
 * to the current row with no possibility to change row. The data of
 * the row can be accessed though the next<Type> methods which return
 * the optional value of the next column.
 */
class ResultSetRow( val rs: ResultSet ) {
    /** Maintain the current position. */
    private var position = 0
      
    def nextBoolean: Option[ Boolean ] = nextValueOption( rs.getBoolean )
    def nextInt: Option[ Int ] = nextValueOption( rs.getInt )
    def nextLong: Option[ Long ] = nextValueOption( rs.getLong )
    def nextFloat: Option[ Float ] = nextValueOption( rs.getFloat )
    def nextDouble: Option[ Double ] = nextValueOption( rs.getDouble )
    def nextString: Option[ String ] = nextValueOption( rs.getString )
    def nextDate: Option[ Date ] =  nextValueOption( rs.getTimestamp )
    def nextObject: Option[ AnyRef ] = nextValueOption( rs.getObject )
    def nextBinary: Option[ Array[Byte] ] = nextValueOption( rs.getBytes )
    def nextBigDecimal: Option[ BigDecimal ] = nextValueOption( i => BigDecimal(rs.getBigDecimal(i)) )

    def boolean(column: String): Option[ Boolean ] = getColumnValueOption( column, rs.getBoolean )
    def int(column: String): Option[ Int ] = getColumnValueOption( column, rs.getInt )
    def long(column: String): Option[ Long ] = getColumnValueOption( column, rs.getLong )
    def float(column: String): Option[ Float ] = getColumnValueOption( column, rs.getFloat )
    def double(column: String): Option[ Double ] = getColumnValueOption( column, rs.getDouble )
    def string(column: String): Option[ String ] = getColumnValueOption( column, rs.getString )
    def date(column: String): Option[ Date ] =  getColumnValueOption( column, rs.getTimestamp )
    def binary(column: String): Option[ Array[Byte] ] = getColumnValueOption( column, rs.getBytes )
    def bigDecimal(column: String): Option[ BigDecimal ] = getColumnValueOption( column, c => BigDecimal( rs.getBigDecimal(c) ) )

    def boolean(index: Int): Option[ Boolean ] = getIndexValueOption( index, rs.getBoolean )
    def int(index: Int): Option[ Int ] = getIndexValueOption( index, rs.getInt )
    def long(index: Int): Option[ Long ] = getIndexValueOption( index, rs.getLong )
    def float(index: Int): Option[ Float ] = getIndexValueOption( index, rs.getFloat )
    def double(index: Int): Option[ Double ] = getIndexValueOption( index, rs.getDouble )
    def string(index: Int): Option[ String ] = getIndexValueOption( index, rs.getString )
    def date(index: Int): Option[ Date ] =  getIndexValueOption( index, rs.getTimestamp )
    def binary(index: Int): Option[ Array[Byte] ] = getIndexValueOption( index, rs.getBytes )
    def bigDecimal(index: Int): Option[ BigDecimal ] = getIndexValueOption( index, i => BigDecimal( rs.getBigDecimal(i) ) )

    def columnNames: Seq[ String ]= {          
        val columnNames = ArrayBuffer.empty[ String ]
        val metaData = rs.getMetaData
        for(index <- 0.until( metaData.getColumnCount ) ) {
            columnNames += metaData.getColumnName( index + 1 ).toLowerCase
        }
        columnNames
    }
    
    private def incrementPosition = { 
        position = position + 1 
    }
    
    private def getColumnValueOption[T]( column: String, f: (String) => T ): Option[ T ] = {
        val value = f( column )
        if( rs.wasNull ) None
        else Some( value )
    }

    private def getIndexValueOption[T]( index: Int, f: (Int) => T ): Option[ T ] = {
        val value = f( index )
        if( rs.wasNull ) None
        else Some( value )
    }

    private def nextValueOption[T]( f: (Int) => T ): Option[ T ] = {
        incrementPosition
        val value = f( position )
        if( rs.wasNull ) None
        else Some( value )
    }
}

object ResultSetRow {
    
    def apply( rs: ResultSet ): ResultSetRow = {
        new ResultSetRow( rs )
    }
}
/**
 * Defines a number of implicit conversion methods for the supported ColumnTypes. A call
 * to one of these methods will return the next value of the right type. The methods make
 * it easy to step through a row in order to build an object from it as shown in the example
 * below.
 * 
 * Handles all types supported by Prequel as well as Option variants of those.
 *
 *     import net.noerd.prequel.ResultSetRowImplicits._
 *
 *     case class Person( id: Long, name: String, birthdate: DateTime )
 *
 *     InTransaction { tx =>
 *         tx.select( "select id, name, birthdate from people" ) { r =>
 *             Person( r, r, r )
 *         }
 *     }
 */
object ResultSetRowImplicits {
    implicit def row2Boolean( row: ResultSetRow ) = BooleanColumnType( row ).nextValue
    implicit def row2Int( row: ResultSetRow ): Int = IntColumnType( row ).nextValue
    implicit def row2Long( row: ResultSetRow ): Long = LongColumnType( row ).nextValue
    implicit def row2Float( row: ResultSetRow ) = FloatColumnType( row ).nextValue
    implicit def row2Double( row: ResultSetRow ) = DoubleColumnType( row ).nextValue
    implicit def row2String( row: ResultSetRow ) = StringColumnType( row ).nextValue
    implicit def row2Date( row: ResultSetRow ) = DateColumnType( row ).nextValue
    implicit def row2DateTime( row: ResultSetRow ) = DateTimeColumnType( row ).nextValue
    implicit def row2Duration( row: ResultSetRow ) = DurationColumnType( row ).nextValue
    implicit def row2Binary( row: ResultSetRow ) = BinaryColumnType( row ).nextValue
    implicit def row2BigDecimal( row: ResultSetRow ) = BigDecimalColumnType( row ).nextValue

    implicit def row2BooleanOption( row: ResultSetRow ) = BooleanColumnType( row ).nextValueOption
    implicit def row2IntOption( row: ResultSetRow ) = IntColumnType( row ).nextValueOption
    implicit def row2LongOption( row: ResultSetRow ) = LongColumnType( row ).nextValueOption
    implicit def row2FloatOption( row: ResultSetRow ) = FloatColumnType( row ).nextValueOption
    implicit def row2DoubleOption( row: ResultSetRow ) = DoubleColumnType( row ).nextValueOption
    implicit def row2StringOption( row: ResultSetRow ) = StringColumnType( row ).nextValueOption
    implicit def row2DateOption( row: ResultSetRow ) = DateColumnType( row ).nextValueOption
    implicit def row2DateTimeOption( row: ResultSetRow ) = DateTimeColumnType( row ).nextValueOption
    implicit def row2DurationOption( row: ResultSetRow ) = DurationColumnType( row ).nextValueOption
    implicit def row2BinaryOption( row: ResultSetRow ) = BinaryColumnType( row ).nextValueOption
    implicit def row2BigDecimalOption( row: ResultSetRow ) = BigDecimalColumnType( row ).nextValueOption
}