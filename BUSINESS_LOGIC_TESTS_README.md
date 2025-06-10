# Business Logic Test Suite

This comprehensive test suite validates the core business logic of the enrichment calculation system. It focuses exclusively on testing the mathematical accuracy, data processing logic, and error handling robustness without relying on external dependencies like AI services, databases, or file I/O.

## 🎯 Purpose

The test suite ensures that:
- ✅ All enrichment calculations are mathematically correct
- ✅ Data type detection and format analysis works reliably
- ✅ Edge cases and error conditions are handled gracefully
- ✅ The system behaves predictably with various data patterns
- ✅ Performance characteristics meet expectations

## 📁 Test Files

### `test_enrichment_business_logic.py`
Comprehensive tests for the `EnrichmentStatisticsCalculator` class covering:
- Basic statistics calculation (row counts, column counts, etc.)
- Global statistics with many-to-one mappings
- Column comparison statistics (discarded, added, fixed, good data)
- Records modified tracking across multiple mappings
- Percentage calculations and zero-division protection
- Format detection integration
- Phone number validation logic
- Unicode and special character handling
- Large dataset behavior simulation
- Error handling for missing columns and malformed data

### `test_inconsistency_analysis.py`
Comprehensive tests for the `analyze_inconsistency` function and related logic:
- Data type detection (email, URL, phone, boolean, integer, float, string)
- Format count calculation for each data type
- Threshold-based classification logic
- Regex pattern validation for various formats
- Date format detection with multiple format support
- Phone number validation (with/without phonenumbers library)
- URL format analysis and categorization
- Boolean format detection (true/false, yes/no, 1/0, etc.)
- Numeric format detection with separators and scientific notation
- Empty data and null value handling
- Mixed data type scenarios
- Performance with repetitive data

## 🚀 Quick Start

### 1. Install Dependencies
```bash
pip install -r test_requirements.txt
```

### 2. Run All Tests
```bash
python run_business_logic_tests.py
```

### 3. Run Individual Test Suites
```bash
# Enrichment calculator tests only
python -m pytest test_enrichment_business_logic.py -v

# Inconsistency analysis tests only
python -m pytest test_inconsistency_analysis.py -v

# Both with detailed output
python -m pytest test_enrichment_business_logic.py test_inconsistency_analysis.py -v --tb=long
```

## 📊 Test Coverage Areas

### Core Business Logic
- **Data Enrichment Calculations**: Row counts, column mapping statistics, modification tracking
- **Global Statistics**: Many-to-one relationships, column reduction calculations
- **Column Comparison Logic**: Categorizing data changes (good, fixed, added, discarded)
- **Percentage Calculations**: Before/after correctness percentages with zero-division protection

### Data Type Detection & Format Analysis
- **Email Detection**: RFC-compliant email pattern matching
- **URL Analysis**: Scheme detection, www prefix, path analysis
- **Phone Number Validation**: International format support, extension handling
- **Date Format Detection**: Multiple format support (ISO, US, European, with time)
- **Boolean Recognition**: Various boolean representations (true/false, yes/no, 1/0, etc.)
- **Numeric Analysis**: Integer vs float distinction, separator handling, scientific notation
- **Format Counting**: Detecting multiple formats within the same data type

### Error Handling & Edge Cases
- **Empty Data**: Null DataFrames, empty columns, all-null data
- **Malformed Input**: Invalid column mappings, missing columns
- **Unicode Support**: International characters, emojis, special symbols
- **Large Datasets**: Performance testing with 1000+ rows
- **Mixed Data Types**: Handling columns with multiple data types
- **Threshold Testing**: Classification behavior at different confidence thresholds

### Performance & Robustness
- **Memory Efficiency**: Handling large datasets without memory issues
- **Processing Speed**: Reasonable performance with complex data patterns
- **Warning Suppression**: Clean execution without unnecessary warnings
- **Exception Handling**: Graceful failure recovery

## 🧪 Test Patterns and Methodologies

### Mock Usage
External dependencies are mocked to focus on business logic:
- **AI Services**: Column matching responses are mocked with known data
- **Phone Libraries**: phonenumbers library behavior is simulated
- **Database Operations**: Not tested (outside scope of business logic)
- **File I/O**: Not tested (outside scope of business logic)

### Data Patterns Tested
- **Real-world Data**: Company names, phone numbers, email addresses
- **Edge Cases**: Empty strings, whitespace, null values, mixed types
- **Unicode Data**: International characters, emojis, special symbols
- **Large Numbers**: Scientific notation, very large integers
- **Malformed Data**: Invalid formats, corrupted entries

### Assertion Strategies
- **Exact Matches**: For deterministic calculations
- **Range Checks**: For percentage calculations and statistics
- **Pattern Verification**: For format detection results
- **Error Condition Testing**: Ensuring proper exception handling

## 📈 Success Criteria

A successful test run validates that:

1. **Mathematical Accuracy**: All calculations produce correct results
2. **Data Integrity**: No data is lost or corrupted during processing
3. **Format Detection**: Data types are correctly identified
4. **Error Resilience**: System handles edge cases gracefully
5. **Performance**: Reasonable execution time with large datasets
6. **Unicode Support**: International data is processed correctly

## 🔧 Debugging Failed Tests

### Common Issues and Solutions

**Import Errors**:
```bash
# Install missing dependencies
pip install -r test_requirements.txt
```

**Path Issues**:
```bash
# Run tests from the same directory as the test files
cd /path/to/test/directory
python run_business_logic_tests.py
```

**Mock-related Failures**:
- Check that external dependencies are properly mocked
- Verify that test data matches expected formats
- Ensure mock return values are realistic

**Calculation Discrepancies**:
- Review test data for edge cases (null values, empty strings)
- Check percentage calculations for zero-division scenarios
- Verify row counting logic with complex data patterns

### Detailed Test Output
```bash
# Get more detailed output for debugging
python -m pytest test_enrichment_business_logic.py -v -s --tb=long

# Run specific test methods
python -m pytest test_enrichment_business_logic.py::TestEnrichmentStatisticsCalculator::test_calculate_statistics_basic_scenario -v
```

## 🎯 Integration with Development Workflow

### Pre-commit Testing
```bash
# Add to your pre-commit hooks
python run_business_logic_tests.py
```

### Continuous Integration
```yaml
# Example GitHub Actions step
- name: Run Business Logic Tests
  run: |
    pip install -r test_requirements.txt
    python run_business_logic_tests.py
```

### Local Development
```bash
# Quick validation during development
python -m pytest test_enrichment_business_logic.py::TestEnrichmentStatisticsCalculator::test_percentage_calculations -v
```

## 📝 Test Data Philosophy

The test suite uses:
- **Synthetic Data**: Crafted to test specific scenarios
- **Realistic Patterns**: Based on actual CRM/export data patterns
- **Edge Cases**: Boundary conditions and error scenarios
- **Unicode Examples**: International and special character testing
- **Large Datasets**: Performance and scalability validation

## 🔄 Maintenance Guidelines

### Adding New Tests
1. Follow existing naming conventions (`test_<functionality>_<scenario>`)
2. Include docstrings explaining the test purpose
3. Use realistic test data when possible
4. Add both positive and negative test cases
5. Update the test runner report if adding new test areas

### Updating Existing Tests
1. Maintain backward compatibility with existing assertions
2. Update test data to reflect new business requirements
3. Ensure mock objects match real-world API changes
4. Verify that edge case coverage is maintained

### Performance Considerations
1. Keep individual tests under 1 second execution time
2. Use appropriate dataset sizes for testing
3. Mock expensive operations (AI calls, database queries)
4. Profile tests if performance degrades

## 🎉 Expected Output

A successful test run produces:
```
🧪 ENRICHMENT BUSINESS LOGIC TEST SUITE
============================================================
✅ All dependencies are available
✅ Enrichment Calculator Business Logic Tests - PASSED
✅ Inconsistency Analysis Business Logic Tests - PASSED
✅ All Business Logic Tests Combined - PASSED

🎉 ALL BUSINESS LOGIC TESTS PASSED!
✅ Enrichment calculations are mathematically correct
✅ Format detection logic is robust
✅ Error handling covers edge cases
✅ Code is ready for production use
```

This comprehensive test suite provides confidence that the enrichment calculation system will behave correctly and reliably in production environments.