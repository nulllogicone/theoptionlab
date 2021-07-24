using Microsoft.Azure.Cosmos.Table;

public class OptionDataRecord : TableEntity
{
    // PartitionKey and RowKey must be set before using an instance
    // PartitionKey = <underlying_symbol>+<option_type> (example)
    // RowKey = <quote_date>

    /// <summary>
    /// Where does the record come from? 
    /// </summary>
    public string Source { get; set; }

    // All properties below come from the data csv import
    // TODO: Nicer mapping to dotnet names
    public string underlying_symbol { get; set; }
    public string quote_date { get; set; }
    public string root { get; set; }
    public string expiration { get; set; }
    public string strike { get; set; }
    public string option_type { get; set; }
    public double open { get; set; }           // todo: what are the data types?
    public string high { get; set; }
    public string low { get; set; }
    public string close { get; set; }
    public string trade_volume { get; set; }
    public string bid_size_1545 { get; set; }
    public string bid_1545 { get; set; }
    public string ask_size_1545 { get; set; }
    public string ask_1545 { get; set; }
    public string underlying_bid_1545 { get; set; }
    public string underlying_ask_1545 { get; set; }
    public string bid_size_eod { get; set; }
    public string bid_eod { get; set; }
    public string ask_size_eod { get; set; }
    public double ask_eod { get; set; }
    public string underlying_bid_eod { get; set; }
    public string underlying_ask_eod { get; set; }
    public string vwap { get; set; }
    public string open_interest { get; set; }
    public string delivery_code { get; set; }
}