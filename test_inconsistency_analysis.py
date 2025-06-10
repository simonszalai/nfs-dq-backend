import pytest
import pandas as pd
from unittest.mock import patch, Mock
import warnings

from app.inconsistency import (
    analyze_inconsistency,
    _detect_series,
    _get_url_format,
    _get_phone_format,
    _get_date_format,
    _get_boolean_format,
    _get_integer_format,
    _get_float_format,
    _detect_date_format,
    _is_valid_phone_number,
    ClassifiedColumn,
    BOOLEAN_VALUES,
    URL_RE,
    EMAIL_RE,
    PHONE_RE,
    DATE_FORMATS
)


class TestAnalyzeInconsistency:
    """Test suite for the main analyze_inconsistency function."""

    def test_analyze_inconsistency_basic_types(self):
        """Test basic data type detection."""
        df = pd.DataFrame({
            'emails': ['test@example.com', 'user@test.com', 'admin@site.org'],
            'urls': ['https://example.com', 'www.test.com', 'http://site.org'],
            'phones': ['+1-555-123-4567', '555-987-6543', '(555) 111-2222'],
            'booleans': ['true', 'false', 'yes'],
            'integers': ['123', '456', '789'],
            'floats': ['123.45', '67.89', '0.123'],
            'strings': ['apple', 'banana', 'cherry']
        })

        result = analyze_inconsistency(df)

        assert 'emails' in result
        assert result['emails'].type == 'email'
        
        assert 'urls' in result
        assert result['urls'].type == 'url'
        
        assert 'booleans' in result
        assert result['booleans'].type == 'boolean'
        
        assert 'integers' in result
        assert result['integers'].type == 'integer'
        
        assert 'floats' in result
        assert result['floats'].type == 'float'
        
        assert 'strings' in result
        assert result['strings'].type == 'string'

    def test_analyze_inconsistency_mixed_data(self):
        """Test with mixed data that doesn't meet threshold."""
        df = pd.DataFrame({
            'mixed': ['test@example.com', 'not-email', 'another-string', 'regular-text']
        })

        result = analyze_inconsistency(df, threshold=0.8)

        # Only 25% are emails, below 80% threshold, should default to string
        assert result['mixed'].type == 'string'

    def test_analyze_inconsistency_empty_columns(self):
        """Test with empty or null columns."""
        df = pd.DataFrame({
            'empty': [],
            'nulls': [None, None, None],
            'mixed_nulls': ['value1', None, 'value2']
        })

        result = analyze_inconsistency(df)

        # Empty and null columns should default to string
        assert result['empty'].type == 'string'
        assert result['nulls'].type == 'string'
        assert result['mixed_nulls'].type == 'string'

    def test_analyze_inconsistency_custom_threshold(self):
        """Test with custom threshold values."""
        df = pd.DataFrame({
            'mostly_emails': ['test@example.com', 'user@test.com', 'not-email']
        })

        # With high threshold (0.9), should not detect as email
        result_high = analyze_inconsistency(df, threshold=0.9)
        assert result_high['mostly_emails'].type == 'string'

        # With lower threshold (0.5), should detect as email
        result_low = analyze_inconsistency(df, threshold=0.5)
        assert result_low['mostly_emails'].type == 'email'

    def test_analyze_inconsistency_format_counts(self):
        """Test format count detection for various data types."""
        df = pd.DataFrame({
            'urls_varied': [
                'https://example.com',
                'http://test.com', 
                'www.site.org',
                'example.net'
            ],
            'phones_varied': [
                '+1-555-123-4567',
                '(555) 987-6543',
                '555.111.2222',
                '5551234567'
            ]
        })

        result = analyze_inconsistency(df)

        # Should detect multiple formats for URLs and phones
        assert result['urls_varied'].type == 'url'
        assert result['urls_varied'].format_count > 1

        assert result['phones_varied'].type == 'phone'
        assert result['phones_varied'].format_count > 1


class TestDetectSeries:
    """Test suite for the _detect_series function."""

    def test_detect_series_urls(self):
        """Test URL detection."""
        url_series = pd.Series([
            'https://example.com',
            'http://test.com',
            'www.site.org',
            'example.net'
        ])

        dtype, format_count = _detect_series(url_series, 0.8)
        assert dtype == 'url'
        assert format_count >= 1

    def test_detect_series_emails(self):
        """Test email detection."""
        email_series = pd.Series([
            'test@example.com',
            'user@test.com',
            'admin@site.org'
        ])

        dtype, format_count = _detect_series(email_series, 0.8)
        assert dtype == 'email'
        assert format_count is None  # Emails don't have meaningful format variations

    @patch('app.inconsistency.phonenumbers')
    def test_detect_series_phones_with_library(self, mock_phonenumbers):
        """Test phone detection with phonenumbers library available."""
        mock_phonenumbers.parse.return_value = Mock()
        mock_phonenumbers.is_valid_number.return_value = True

        phone_series = pd.Series([
            '+1-555-123-4567',
            '555-987-6543',
            '(555) 111-2222'
        ])

        dtype, format_count = _detect_series(phone_series, 0.8)
        assert dtype == 'phone'
        assert format_count >= 1

    def test_detect_series_phones_without_library(self):
        """Test phone detection without phonenumbers library."""
        with patch('app.inconsistency.phonenumbers', None):
            phone_series = pd.Series([
                '+1-555-123-4567',
                '555-987-6543',
                '(555) 111-2222'
            ])

            dtype, format_count = _detect_series(phone_series, 0.8)
            assert dtype == 'phone'
            assert format_count >= 1

    def test_detect_series_dates(self):
        """Test date detection."""
        date_series = pd.Series([
            '2023-01-01',
            '2023-02-15',
            '2023-12-31'
        ])

        dtype, format_count = _detect_series(date_series, 0.8)
        assert dtype == 'date'
        assert format_count >= 1

    def test_detect_series_booleans(self):
        """Test boolean detection."""
        bool_series = pd.Series(['true', 'false', 'yes', 'no'])

        dtype, format_count = _detect_series(bool_series, 0.8)
        assert dtype == 'boolean'
        assert format_count >= 1

    def test_detect_series_integers(self):
        """Test integer detection."""
        int_series = pd.Series(['123', '456', '789', '1,000'])

        dtype, format_count = _detect_series(int_series, 0.8)
        assert dtype == 'integer'
        assert format_count >= 1

    def test_detect_series_floats(self):
        """Test float detection."""
        float_series = pd.Series(['123.45', '67.89', '0.123'])

        dtype, format_count = _detect_series(float_series, 0.8)
        assert dtype == 'float'
        assert format_count >= 1

    def test_detect_series_empty(self):
        """Test detection with empty series."""
        empty_series = pd.Series([], dtype=object)

        dtype, format_count = _detect_series(empty_series, 0.8)
        assert dtype is None
        assert format_count is None

    def test_detect_series_below_threshold(self):
        """Test detection when data doesn't meet threshold."""
        mixed_series = pd.Series([
            'test@example.com',  # Email
            'not-email',
            'another-string',
            'regular-text'
        ])

        dtype, format_count = _detect_series(mixed_series, 0.8)
        # Only 25% emails, below 80% threshold
        assert dtype == 'string'
        assert format_count is None


class TestFormatDetectionFunctions:
    """Test suite for individual format detection functions."""

    def test_get_url_format(self):
        """Test URL format detection."""
        assert 'scheme:https' in _get_url_format('https://example.com')
        assert 'scheme:http' in _get_url_format('http://example.com')
        assert 'no_scheme' in _get_url_format('example.com')
        assert 'www' in _get_url_format('https://www.example.com')
        assert 'no_www' in _get_url_format('https://example.com')
        assert 'has_path' in _get_url_format('https://example.com/path')
        assert 'no_path' in _get_url_format('https://example.com')

    def test_get_phone_format(self):
        """Test phone format detection."""
        # Test country code
        assert 'plus' in _get_phone_format('+1-555-123-4567')
        
        # Test parentheses
        assert 'parens' in _get_phone_format('(555) 123-4567')
        
        # Test separators
        assert 'dash' in _get_phone_format('555-123-4567')
        assert 'dot' in _get_phone_format('555.123.4567')
        assert 'space' in _get_phone_format('555 123 4567')
        assert 'nosep' in _get_phone_format('5551234567')
        
        # Test extensions
        assert 'ext' in _get_phone_format('555-123-4567 ext 123')
        assert 'ext' in _get_phone_format('555-123-4567 x123')

    def test_get_boolean_format(self):
        """Test boolean format detection."""
        assert _get_boolean_format('true') == 'true_false'
        assert _get_boolean_format('false') == 'true_false'
        assert _get_boolean_format('yes') == 'yes_no'
        assert _get_boolean_format('no') == 'yes_no'
        assert _get_boolean_format('y') == 'y_n'
        assert _get_boolean_format('n') == 'y_n'
        assert _get_boolean_format('1') == '1_0'
        assert _get_boolean_format('0') == '1_0'
        assert _get_boolean_format('on') == 'on_off'
        assert _get_boolean_format('off') == 'on_off'
        assert _get_boolean_format('t') == 't_f'
        assert _get_boolean_format('f') == 't_f'
        assert _get_boolean_format('unknown') == 'unknown'

    def test_get_integer_format(self):
        """Test integer format detection."""
        assert _get_integer_format('123') == 'plain'
        assert _get_integer_format('1,000') == 'comma_separated'
        assert _get_integer_format('1_000') == 'underscore_separated'

    def test_get_float_format(self):
        """Test float format detection."""
        assert _get_float_format('123.45') == 'plain'
        assert _get_float_format('1,234.56') == 'comma_thousands|period_decimal'
        assert _get_float_format('1.234,56') == 'comma_decimal'
        assert _get_float_format('1_234.56') == 'underscore_thousands|period_decimal'
        assert _get_float_format('1.23e10') == 'period_decimal|scientific'
        assert _get_float_format('1.23E-5') == 'period_decimal|scientific'


class TestDateDetection:
    """Test suite for date detection functions."""

    def test_detect_date_format_common_formats(self):
        """Test detection of common date formats."""
        # ISO format
        iso_dates = pd.Series(['2023-01-01', '2023-02-15', '2023-12-31'])
        format_result = _detect_date_format(iso_dates)
        assert format_result is not None

        # US format
        us_dates = pd.Series(['01/15/2023', '02/28/2023', '12/31/2023'])
        format_result = _detect_date_format(us_dates)
        assert format_result is not None

        # European format
        eu_dates = pd.Series(['15/01/2023', '28/02/2023', '31/12/2023'])
        format_result = _detect_date_format(eu_dates)
        assert format_result is not None

    def test_detect_date_format_mixed_formats(self):
        """Test with mixed date formats."""
        mixed_dates = pd.Series([
            '2023-01-01',   # ISO
            '01/15/2023',   # US
            'not-a-date',   # Invalid
            '2023-12-31'    # ISO
        ])
        
        format_result = _detect_date_format(mixed_dates)
        # Should still detect some date format if at least 50% are valid
        assert format_result is not None

    def test_detect_date_format_no_dates(self):
        """Test with non-date data."""
        non_dates = pd.Series(['apple', 'banana', 'cherry'])
        format_result = _detect_date_format(non_dates)
        assert format_result is None

    def test_detect_date_format_with_time(self):
        """Test date detection with time components."""
        datetime_series = pd.Series([
            '2023-01-01 12:00:00',
            '2023-02-15 15:30:45',
            '2023-12-31 23:59:59'
        ])
        
        format_result = _detect_date_format(datetime_series)
        assert format_result is not None

    def test_detect_date_format_edge_cases(self):
        """Test edge cases for date detection."""
        # Empty series
        empty_series = pd.Series([], dtype=object)
        assert _detect_date_format(empty_series) is None

        # All nulls
        null_series = pd.Series([None, None, None])
        assert _detect_date_format(null_series) is None

        # Single date
        single_date = pd.Series(['2023-01-01'])
        assert _detect_date_format(single_date) is not None


class TestPhoneNumberValidation:
    """Test suite for phone number validation."""

    @patch('app.inconsistency.phonenumbers')
    def test_is_valid_phone_number_with_library(self, mock_phonenumbers):
        """Test phone validation with phonenumbers library."""
        # Mock successful validation
        mock_phonenumbers.parse.return_value = Mock()
        mock_phonenumbers.is_valid_number.return_value = True

        result = _is_valid_phone_number('+1-555-123-4567')
        assert result is True

        # Mock failed validation
        mock_phonenumbers.parse.side_effect = Exception("Parse error")
        result = _is_valid_phone_number('invalid-phone')
        assert result is False

    def test_is_valid_phone_number_without_library(self):
        """Test phone validation without phonenumbers library."""
        with patch('app.inconsistency.phonenumbers', None):
            result = _is_valid_phone_number('+1-555-123-4567')
            assert result is False

    @patch('app.inconsistency.phonenumbers')
    def test_is_valid_phone_number_edge_cases(self, mock_phonenumbers):
        """Test edge cases for phone number validation."""
        # Test with different regions
        mock_phonenumbers.parse.return_value = Mock()
        mock_phonenumbers.is_valid_number.return_value = True

        result = _is_valid_phone_number('555-123-4567', region='US')
        assert result is True

        # Test with NumberParseException
        from app.inconsistency import NumberParseException
        mock_phonenumbers.parse.side_effect = NumberParseException(1, "Invalid")
        result = _is_valid_phone_number('invalid')
        assert result is False


class TestRegexPatterns:
    """Test suite for regex patterns used in detection."""

    def test_url_regex_pattern(self):
        """Test URL regex pattern."""
        valid_urls = [
            'https://example.com',
            'http://example.com',
            'www.example.com',
            'example.com',
            'example.com/path',
            'example.com/path?query=value'
        ]
        
        invalid_urls = [
            'not-a-url',
            'ftp://example.com',  # Not http/https
            '123.456',  # Invalid TLD
            'example'   # No TLD
        ]

        for url in valid_urls:
            assert URL_RE.match(url), f"Should match: {url}"

        for url in invalid_urls:
            assert not URL_RE.match(url), f"Should not match: {url}"

    def test_email_regex_pattern(self):
        """Test email regex pattern."""
        valid_emails = [
            'test@example.com',
            'user.name@example.org',
            'user+tag@example.net',
            'user123@example123.com'
        ]
        
        invalid_emails = [
            'not-an-email',
            '@example.com',  # Missing local part
            'user@',         # Missing domain
            'user@domain'    # Missing TLD
        ]

        for email in valid_emails:
            assert EMAIL_RE.match(email), f"Should match: {email}"

        for email in invalid_emails:
            assert not EMAIL_RE.match(email), f"Should not match: {email}"

    def test_phone_regex_pattern(self):
        """Test phone regex pattern."""
        valid_phones = [
            '+1-555-123-4567',
            '555-123-4567',
            '(555) 123-4567',
            '555.123.4567',
            '5551234567',
            '+1 555 123 4567',
            '555-123-4567 ext 123'
        ]
        
        invalid_phones = [
            'not-a-phone',
            '123',          # Too short
            'abc-def-ghij'  # Non-numeric
        ]

        for phone in valid_phones:
            assert PHONE_RE.match(phone), f"Should match: {phone}"

        for phone in invalid_phones:
            assert not PHONE_RE.match(phone), f"Should not match: {phone}"


class TestConstantsAndUtilities:
    """Test suite for constants and utility functions."""

    def test_boolean_values_dictionary(self):
        """Test the BOOLEAN_VALUES constant."""
        # Test that all expected boolean representations are present
        expected_keys = [
            'true', 'false', 'yes', 'no', 'y', 'n',
            '1', '0', 'on', 'off', 't', 'f'
        ]
        
        for key in expected_keys:
            assert key in BOOLEAN_VALUES

        # Test some specific mappings
        assert BOOLEAN_VALUES['true'] is True
        assert BOOLEAN_VALUES['false'] is False
        assert BOOLEAN_VALUES['yes'] is True
        assert BOOLEAN_VALUES['no'] is False
        assert BOOLEAN_VALUES['1'] is True
        assert BOOLEAN_VALUES['0'] is False

    def test_date_formats_list(self):
        """Test the DATE_FORMATS list."""
        # Should contain common date formats
        assert '%Y-%m-%d' in DATE_FORMATS  # ISO format
        assert '%m/%d/%Y' in DATE_FORMATS  # US format
        assert '%d/%m/%Y' in DATE_FORMATS  # European format
        assert '%Y-%m-%d %H:%M:%S' in DATE_FORMATS  # With time

        # Should have a reasonable number of formats
        assert len(DATE_FORMATS) > 10

    def test_classified_column_model(self):
        """Test the ClassifiedColumn Pydantic model."""
        # Test default values
        col = ClassifiedColumn(type='string')
        assert col.type == 'string'
        assert col.format_count == 1

        # Test custom values
        col = ClassifiedColumn(type='phone', format_count=3)
        assert col.type == 'phone'
        assert col.format_count == 3


class TestErrorHandlingAndEdgeCases:
    """Test suite for error handling and edge cases."""

    def test_corrupted_data_handling(self):
        """Test handling of corrupted or malformed data."""
        # DataFrame with various problematic data
        df = pd.DataFrame({
            'mixed_types': [1, 'string', 3.14, None, True],
            'unicode_chars': ['café', '北京', '🙂', 'normal'],
            'special_chars': ['@#$%', '***', '---', '|||']
        })

        # Should not raise exceptions
        result = analyze_inconsistency(df)
        
        assert 'mixed_types' in result
        assert 'unicode_chars' in result
        assert 'special_chars' in result
        
        # All should default to string type
        for col_result in result.values():
            assert col_result.type in ['string', 'integer', 'float']  # Mixed types might be detected as numeric

    def test_very_large_numbers(self):
        """Test with very large numbers."""
        df = pd.DataFrame({
            'big_ints': ['999999999999999999', '123456789012345678'],
            'big_floats': ['1.23e50', '9.87e-100']
        })

        result = analyze_inconsistency(df)
        
        # Should detect as appropriate numeric types
        assert result['big_ints'].type in ['integer', 'string']
        assert result['big_floats'].type in ['float', 'string']

    def test_warning_suppression(self):
        """Test that warnings are properly suppressed during date parsing."""
        # This test ensures that the warning suppression in _detect_date_format works
        ambiguous_dates = pd.Series(['01/02/2023', '03/04/2023'])  # Could be interpreted multiple ways
        
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            _detect_date_format(ambiguous_dates)
            
            # Should not have datetime-related warnings
            datetime_warnings = [warning for warning in w 
                               if 'datetime' in str(warning.message).lower() or 
                                  'infer' in str(warning.message).lower()]
            assert len(datetime_warnings) == 0

    def test_performance_with_repetitive_data(self):
        """Test performance characteristics with highly repetitive data."""
        # Create data with many repeated values
        df = pd.DataFrame({
            'repetitive': ['same_value'] * 1000 + ['different_value'] * 10
        })

        result = analyze_inconsistency(df)
        
        # Should handle repetitive data efficiently
        assert result['repetitive'].type == 'string'
        assert result['repetitive'].format_count == 1

    def test_null_vs_empty_string_distinction(self):
        """Test distinction between null values and empty strings."""
        df = pd.DataFrame({
            'mixed_nulls': [None, '', '   ', 'value', pd.NA]
        })

        result = analyze_inconsistency(df)
        
        # Should handle mixed null types without errors
        assert 'mixed_nulls' in result
        assert result['mixed_nulls'].type == 'string'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])