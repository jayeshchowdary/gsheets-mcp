# Pagination Guide for Google Sheets MCP Tools

This guide explains how to use the new pagination features implemented in the Google Sheets MCP Server tools for a smoother user experience.

## 🎯 **What's Been Implemented**

### **1. list_sheets Tool**
- **New Parameters:**
  - `max_results`: Maximum sheets to return (1-1000, default: 50)
  - `page_token`: Token for next page (from previous response)

- **Response Structure:**
  ```json
  {
    "successful": true,
    "message": "Found X Google Sheets (showing up to Y)",
    "sheets": [...],
    "pagination": {
      "has_more": true/false,
      "next_page_token": "token_string_or_null",
      "total_estimated": 100
    },
    "summary": {
      "returned_count": 50,
      "max_results": 50,
      "page_number": 1
    }
  }
  ```

- **Usage Example:**
  ```python
  # First page
  result1 = await list_sheets(max_results=10)
  
  # Check if more results exist
  if result1["pagination"]["has_more"]:
      # Get next page
      next_token = result1["pagination"]["next_page_token"]
      result2 = await list_sheets(max_results=10, page_token=next_token)
  ```

### **2. search_spreadsheets Tool**
- **Enhanced Parameters:**
  - `max_results`: Maximum results to return (1-1000, default: 10)
  - `page_token`: Token for next page (from previous response)

- **Response Structure:**
  ```json
  {
    "successful": true,
    "message": "Found X spreadsheet(s) matching the search criteria (showing up to Y)",
    "spreadsheets": [...],
    "pagination": {
      "has_more": true/false,
      "next_page_token": "token_string_or_null",
      "total_estimated": 50
    },
    "search_parameters": {...},
    "summary": {...}
  }
  ```

- **Usage Example:**
  ```python
  # Search with pagination
  result1 = await search_spreadsheets(
      query="name contains 'budget'", 
      max_results=5
  )
  
  # Get next page if available
  if result1["pagination"]["has_more"]:
      next_token = result1["pagination"]["next_page_token"]
      result2 = await search_spreadsheets(
          query="name contains 'budget'", 
          max_results=5,
          page_token=next_token
      )
  ```

### **3. get_sheet_names Tool**
- **New Parameters:**
  - `max_sheets`: Maximum sheets to return (1-1000, default: 100)
  - `start_index`: Starting index for pagination (0-based, default: 0)

- **Response Structure:**
  ```json
  {
    "successful": true,
    "message": "Found X worksheet(s) in spreadsheet 'Title' (showing Y starting from index Z)",
    "sheet_names": [...],
    "sheet_count": 50,
    "sheet_details": [...],
    "spreadsheet_info": {...},
    "pagination": {
      "has_more": true/false,
      "next_start_index": 100,
      "total_sheets": 150,
      "returned_count": 50,
      "start_index": 0,
      "end_index": 49
    }
  }
  ```

- **Usage Example:**
  ```python
  # First page
  result1 = await get_sheet_names(
      spreadsheet_id="your_id", 
      max_sheets=20
  )
  
  # Next page
  if result1["pagination"]["has_more"]:
      next_start = result1["pagination"]["next_start_index"]
      result2 = await get_sheet_names(
          spreadsheet_id="your_id", 
          max_sheets=20,
          start_index=next_start
      )
  ```

### **4. list_tables Tool**
- **New Parameters:**
  - `max_tables`: Maximum tables to return (1-1000, default: 50)
  - `start_index`: Starting index for pagination (0-based, default: 0)

- **Response Structure:**
  ```json
  {
    "successful": true,
    "message": "Found X table(s) across Y sheet(s) (showing Z starting from index W)",
    "tables": [...],
    "total_tables": 25,
    "total_sheets": 3,
    "pagination": {
      "has_more": true/false,
      "next_start_index": 50,
      "total_tables": 100,
      "returned_count": 25,
      "start_index": 0,
      "end_index": 24
    },
    "analysis_parameters": {...},
    "summary": {...}
  }
  ```

- **Usage Example:**
  ```python
  # First page of tables
  result1 = await list_tables(
      spreadsheet_id="your_id", 
      max_tables=25
  )
  
  # Next page
  if result1["pagination"]["has_more"]:
      next_start = result1["pagination"]["next_start_index"]
      result2 = await list_tables(
          spreadsheet_id="your_id", 
          max_tables=25,
          start_index=next_start
      )
  ```

## 🔄 **Pagination Types**

### **Google Drive API Pagination (list_sheets, search_spreadsheets)**
- Uses `page_token` for seamless pagination
- Automatically handles Google's internal pagination
- More efficient for large datasets
- **Best for:** When you need to iterate through many results

### **Manual Pagination (get_sheet_names, list_tables)**
- Uses `start_index` for manual control
- Gives you precise control over which results to get
- Useful for jumping to specific sections
- **Best for:** When you need specific ranges or want to skip results

## 📋 **Best Practices**

### **1. Choose Appropriate Page Sizes**
- **Small pages (10-25)**: Better for real-time user interaction
- **Medium pages (50-100)**: Good balance between performance and user experience
- **Large pages (200+)**: Better for bulk operations, but may be slower

### **2. Handle Pagination Gracefully**
```python
async def get_all_sheets():
    all_sheets = []
    page_token = None
    
    while True:
        result = await list_sheets(max_results=50, page_token=page_token)
        
        if not result["successful"]:
            break
            
        all_sheets.extend(result["sheets"])
        
        if not result["pagination"]["has_more"]:
            break
            
        page_token = result["pagination"]["next_page_token"]
    
    return all_sheets
```

### **3. Cache Pagination Tokens**
- Store `next_page_token` values if you need to resume later
- Useful for long-running operations or user sessions
- Be aware that tokens may expire

### **4. Error Handling**
```python
try:
    result = await list_sheets(max_results=50)
    if result["successful"]:
        # Process results
        if result["pagination"]["has_more"]:
            # Store token for next page
            next_token = result["pagination"]["next_page_token"]
    else:
        print(f"Error: {result['message']}")
except Exception as e:
    print(f"Exception: {e}")
```

## 🚀 **Performance Benefits**

### **Before Pagination:**
- All results returned at once
- Potential memory issues with large datasets
- Slower response times
- User waits for complete results

### **After Pagination:**
- Results returned in manageable chunks
- Better memory management
- Faster initial response times
- Progressive loading for better UX

## 🔧 **Testing Pagination**

### **Test with demonstrate_pagination Tool**
```python
# Use the built-in demonstration tool
result = await demonstrate_pagination()
print(result["pagination_examples"])
```

### **Manual Testing**
```python
# Test list_sheets pagination
result1 = await list_sheets(max_results=5)
print(f"First page: {len(result1['sheets'])} sheets")
print(f"Has more: {result1['pagination']['has_more']}")

if result1["pagination"]["has_more"]:
    result2 = await list_sheets(
        max_results=5, 
        page_token=result1["pagination"]["next_page_token"]
    )
    print(f"Second page: {len(result2['sheets'])} sheets")
```

## 📚 **Additional Resources**

- **README.md**: Full project documentation
- **QUICKSTART.md**: Quick setup guide
- **Makefile**: Convenience commands for testing
- **setup.py**: Automated setup and verification

---

## 🔧 **Recent Improvements**

### **Table Detection Fixes**
The `list_tables` tool has been significantly improved to correctly detect separate tables:

- **Before**: Detected overlapping tables and incorrect boundaries
- **After**: Correctly identifies separate tables with proper ranges
- **Example**: Your spreadsheet now correctly shows:
  - Table 1: Electronic Items (A1:F8)
  - Table 2: Shoes (A12:G19)

### **Enhanced Detection Logic**
- **Empty Row Separation**: Tables separated by empty rows are now properly detected
- **Boundary Detection**: Each table's exact start and end positions are accurately identified
- **No More Overlaps**: Tables no longer overlap or include each other's data

**🎉 With these pagination features and table detection improvements, your Google Sheets MCP Server now provides a much smoother and more scalable user experience!**
