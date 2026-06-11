# DATA_FETCHER
fetch data from upstox 

You are expert in fetching and storing data in duckdb in efficient way 
You understand how to effectively fetch data 
THIS BACKEND will read .env for upstox access token
input : UPSTOX ACCESS TOKEN
output : Storing and publishing data in DuckDB converting upstox instrument keys into Human Readble 

APP will act as proxy server for accessing UPSTOX API 
https://api.upstox.com/v2/swagger-ui/index.html
https://api.upstox.com/v2/api-docs
https://upstox.com/developer/api-documentation/example-code/introduction

So user will connect with you for REALTIME Streaming Data OR GET/POST/etc apis will request in human readble key format ,,, you will convert it into correct INSTRUMENT KEYS for UPSTOX APIs and back to normal keys while sending back .
You will also apply caching for regular apis ( any request less than 15 sec can return data from cached /stored data ). Caching may not be applicable for streaming 
Also as streaming may need additional instruments to be added for your end you will make sure to add them.
I have added ExtractInstrumentKeys.py and and test_streamin.py for your reference

PUBLISH your SWAGGER INTERFACE for CHECKING and testing 


[ Client / Frontend ]
       │
       │ (Human-Readable Formats e.g., "RELIANCE", "NIFTY50")
       ▼
┌────────────────────────────────────────────────────────┐
│             FASTAPI MIDDLEWARE PROXY ENGINE            │
│                                                        │
│  1. In-Memory Bidirectional Key Transformer           │
│  2. DuckDB Historical & Cache Storage (<=15s)          │
│  3. Upstox V2 OpenAPI REST Client                      │
│  4. Dynamic WebSocket Stream Hub                       │
└────────────────────────────────────────────────────────┘
       │
       │ (Upstox Instrument Keys e.g., "NSE_EQ|INE002A01018")
       ▼
[ Upstox API V2 Platform ]




 Dynamic F&O Expiry & Strike Resolution MiddlewareContext & ObjectiveYou are instructing a backend coding agent to implement a dynamic Derivatives Translation Layer within the Upstox Proxy server. Upstox relies on static, changing, internal IDs (e.g., NSE_FO|62329). The client application must communicate exclusively using human-readable shorthand strings.The agent must implement a runtime parsing mechanism that uses relative date filtering to dynamically evaluate the correct instrument_key for current expiries, ensuring no static database IDs or keys are ever hardcoded.1. Input/Output Shorthand SpecificationThe agent must parse and validate two primary incoming syntax variants, translating them bidirectionally:A. Futures Contracts (FUT)Inbound Human Format: {UNDERLYING} FUT (e.g., NIFTY FUT, BANKNIFTY FUT, RELIANCE FUT)Target Output: The exact current active month Upstox instrument_key.B. Options Contracts (CE / PE)Inbound Human Format: {UNDERLYING} {STRIKE} {CE/PE} (e.g., NIFTY 24500 CE, BANKNIFTY 52000 PE)Target Output: The exact near-week or near-month expiry Upstox instrument_key.2. Dynamic Algorithmic Filtering RulesTo perform resolutions without hardcoded data, the agent must execute the following logical state machine using the DuckDB instruments table populated from the Upstox master CSV/JSON:Rule 1: Futures Resolution Logic (FUT)When processing a {UNDERLYING} FUT token array:Filter the database fields to isolate records where the segment is exactly NSE_FO, the asset type is FUT, and the underlying ticker matches the user query.Sort all matching records chronologically by their expiration date field in ascending order (ASC).Extract the very first record at index zero. This dynamically guarantees selection of the Current Nearest Month Future contract.Rule 2: Options Resolution Logic (CE / PE)When processing a {UNDERLYING} {STRIKE} {CE/PE} token array:Isolate the dataset where the segment is NSE_FO, the type matches the specified contract style (CE or PE), the underlying matches the requested index/stock, and the strike price value matches the target input numerical casting.Sort the filtered subset chronologically by expiration date in ascending order (ASC).Select the first record in the sorted sequence. This automatically captures the Current Nearest Expiry Option (handling weekly contracts for indices or monthly contracts for equities natively).Rule 3: Outbound Interception & Reverse LookupWhen streaming market feeds or API packets back to the client application:Intercept all raw Upstox dictionary payloads containing an instrument_key.Perform an asynchronous reverse lookup against DuckDB metadata attributes.Re-assemble the object into the matching human syntax ({UNDERLYING} FUT or {UNDERLYING} {STRIKE} {CE/PE}) before it hits the application socket interface.3. Database Optimization & Latency ConstraintsIndexing Mandate: Ensure the DuckDB migration/setup files generate a composite database index covering (underlying_symbol, instrument_type, strike_price, expiry) to keep parsing latencies below 1 millisecond.Error States: If a lookup fails due to an invalid underlying ticker or a strike price that does not exist in the current exchange matrix, the proxy must reject the client query immediately with an explicit, readable validation message.
 You are expert in fetching and storing data in duckdb in efficient way 
You understand how to effectively fetch data 
THIS BACKEND will read .env for upstox access token
input : UPSTOX ACCESS TOKEN
output : Storing and publishing data in DuckDB converting upstox instrument keys into Human Readble 

APP will act as proxy server for accessing UPSTOX API 
https://api.upstox.com/v2/swagger-ui/index.html
https://api.upstox.com/v2/api-docs
https://upstox.com/developer/api-documentation/example-code/introduction

So user will connect with you for REALTIME Streaming Data OR GET/POST/etc apis will request in human readble key format ,,, you will convert it into correct INSTRUMENT KEYS for UPSTOX APIs and back to normal keys while sending back .
You will also apply caching for regular apis ( any request less than 15 sec can return data from cached /stored data ). Caching may not be applicable for streaming 
Also as streaming may need additional instruments to be added for your end you will make sure to add them.
I have added ExtractInstrumentKeys.py and and test_streamin.py for your reference

PUBLISH your SWAGGER INTERFACE for CHECKING and testing .


 Dynamic F&O Expiry & Strike Resolution MiddlewareContext & ObjectiveYou are instructing a backend coding agent to implement a dynamic Derivatives Translation Layer within the Upstox Proxy server. Upstox relies on static, changing, internal IDs (e.g., NSE_FO|62329). The client application must communicate exclusively using human-readable shorthand strings.The agent must implement a runtime parsing mechanism that uses relative date filtering to dynamically evaluate the correct instrument_key for current expiries, ensuring no static database IDs or keys are ever hardcoded.1. Input/Output Shorthand SpecificationThe agent must parse and validate two primary incoming syntax variants, translating them bidirectionally:A. Futures Contracts (FUT)Inbound Human Format: {UNDERLYING} FUT (e.g., NIFTY FUT, BANKNIFTY FUT, RELIANCE FUT)Target Output: The exact current active month Upstox instrument_key.B. Options Contracts (CE / PE)Inbound Human Format: {UNDERLYING} {STRIKE} {CE/PE} (e.g., NIFTY 24500 CE, BANKNIFTY 52000 PE)Target Output: The exact near-week or near-month expiry Upstox instrument_key.2. Dynamic Algorithmic Filtering RulesTo perform resolutions without hardcoded data, the agent must execute the following logical state machine using the DuckDB instruments table populated from the Upstox master CSV/JSON:Rule 1: Futures Resolution Logic (FUT)When processing a {UNDERLYING} FUT token array:Filter the database fields to isolate records where the segment is exactly NSE_FO, the asset type is FUT, and the underlying ticker matches the user query.Sort all matching records chronologically by their expiration date field in ascending order (ASC).Extract the very first record at index zero. This dynamically guarantees selection of the Current Nearest Month Future contract.Rule 2: Options Resolution Logic (CE / PE)When processing a {UNDERLYING} {STRIKE} {CE/PE} token array:Isolate the dataset where the segment is NSE_FO, the type matches the specified contract style (CE or PE), the underlying matches the requested index/stock, and the strike price value matches the target input numerical casting.Sort the filtered subset chronologically by expiration date in ascending order (ASC).Select the first record in the sorted sequence. This automatically captures the Current Nearest Expiry Option (handling weekly contracts for indices or monthly contracts for equities natively).Rule 3: Outbound Interception & Reverse LookupWhen streaming market feeds or API packets back to the client application:Intercept all raw Upstox dictionary payloads containing an instrument_key.Perform an asynchronous reverse lookup against DuckDB metadata attributes.Re-assemble the object into the matching human syntax ({UNDERLYING} FUT or {UNDERLYING} {STRIKE} {CE/PE}) before it hits the application socket interface.3. Database Optimization & Latency ConstraintsIndexing Mandate: Ensure the DuckDB migration/setup files generate a composite database index covering (underlying_symbol, instrument_type, strike_price, expiry) to keep parsing latencies below 1 millisecond.Error States: If a lookup fails due to an invalid underlying ticker or a strike price that does not exist in the current exchange matrix, the proxy must reject the client query immediately with an explicit, readable validation message.

agent default to the closest weekly expiry for options

