# Voice-to-Text Feature Documentation

## Overview
The NLP-to-SQL chatbot includes a comprehensive voice-to-text feature that allows users to speak their database queries instead of typing them. This feature supports multiple languages including native Indian languages, making it accessible to a broader user base.

## Features

### 🎤 Voice Recognition
- **Real-time Speech-to-Text**: Convert spoken words to text instantly
- **Multi-language Support**: Support for 14+ languages including major Indian languages
- **Interim Results**: See text appear as you speak for better user experience
- **Automatic Language Detection**: Smart language processing for better accuracy

### 🌏 Language Support
The voice-to-text feature supports the following languages:

#### English
- 🇺🇸 English (US)
- 🇮🇳 English (India)

#### Indian Languages
- 🇮🇳 हिन्दी (Hindi)
- 🇮🇳 বাংলা (Bengali)
- 🇮🇳 తెలుగు (Telugu)
- 🇮🇳 मराठी (Marathi)
- 🇮🇳 தமிழ் (Tamil)
- 🇮🇳 ગુજરાતી (Gujarati)
- 🇮🇳 ಕನ್ನಡ (Kannada)
- 🇮🇳 മലയാളം (Malayalam)
- 🇮🇳 ਪੰਜਾਬੀ (Punjabi)
- 🇮🇳 ଓଡ଼ିଆ (Odia)
- 🇮🇳 অসমীয়া (Assamese)
- 🇮🇳 اردو (Urdu)

### 🎯 User Interface Features
- **Language Selector**: Easy-to-use dropdown to switch between languages
- **Visual Feedback**: Different button states for ready/listening/error states
- **Progress Indicators**: Animated dots showing listening status
- **Error Handling**: Clear error messages with helpful suggestions
- **Keyboard Shortcuts**: Quick access via keyboard combinations

## How to Use

### Basic Usage
1. **Click the microphone button** in the input area (green button with microphone icon)
2. **Speak your query** clearly into your microphone
3. **Watch the text appear** in real-time as you speak
4. **Press Enter or click Send** to submit your query

### Language Selection
1. **Click the language button** (Languages icon) next to the microphone
2. **Select your preferred language** from the dropdown menu
3. **Start speaking** in the selected language

### Keyboard Shortcuts
- **Ctrl+Shift+V** (Windows/Linux) or **Cmd+Shift+V** (Mac): Start voice input
- **ESC**: Stop listening or close language selector

## Browser Compatibility

### ✅ Fully Supported
- **Chrome/Chromium**: Full support for all features
- **Microsoft Edge**: Full support for all features
- **Opera**: Full support for all features

### ⚠️ Limited Support
- **Firefox**: Basic support (some advanced features may not work)
- **Safari**: Basic support (iOS/macOS versions may vary)

### ❌ Not Supported
- **Internet Explorer**: No support
- **Older browser versions**: May not support Web Speech API

## Technical Requirements

### Hardware
- **Microphone**: Required for voice input
- **Audio permissions**: Browser must have microphone access
- **Stable internet connection**: Required for speech processing

### Software
- **Modern browser**: Chrome 25+, Firefox 44+, Safari 14+, Edge 79+
- **HTTPS connection**: Required for microphone access in most browsers
- **JavaScript enabled**: Required for all functionality

## Troubleshooting

### Common Issues

#### "Microphone permission denied"
- **Solution**: Allow microphone access in your browser settings
- **Steps**: Click the microphone icon in the address bar and select "Allow"

#### "No speech was detected"
- **Solution**: Speak louder and closer to the microphone
- **Check**: Ensure your microphone is working and not muted

#### "Selected language is not supported"
- **Solution**: Switch to a more commonly supported language like English
- **Note**: Some languages may not be available on all devices

#### "Network error occurred"
- **Solution**: Check your internet connection
- **Note**: Voice recognition requires an active internet connection

### Performance Tips

#### For Better Accuracy
- **Speak clearly** and at a moderate pace
- **Use a quiet environment** to reduce background noise
- **Position microphone properly** about 6 inches from your mouth
- **Pause briefly** between sentences for better recognition

#### For Indian Languages
- **Use standard pronunciation** for better recognition
- **Mix with English** when needed (code-switching is supported)
- **Speak technical terms** in English for database-related queries
- **Try different dialects** if recognition is poor

## Security & Privacy

### Data Processing
- **Local Processing**: Speech recognition happens in your browser
- **No Server Storage**: Voice data is not stored on our servers
- **Temporary Processing**: Audio is processed temporarily for recognition only
- **No Recording**: Audio is not recorded or saved anywhere

### Privacy Features
- **Permission-based**: Requires explicit user permission for microphone access
- **Session-based**: Voice permissions reset when browser is closed
- **User Control**: Users can disable voice features at any time

## Advanced Features

### Smart Language Detection
- **Automatic switching**: Detects language changes during speech
- **Mixed language support**: Handles code-switching between languages
- **Context awareness**: Understands database terminology in multiple languages

### Error Recovery
- **Automatic retry**: Attempts to reconnect on network errors
- **Graceful degradation**: Falls back to text input if voice fails
- **User feedback**: Clear error messages with actionable solutions

### Accessibility
- **ARIA labels**: Screen reader compatible
- **Keyboard navigation**: Full keyboard support
- **Visual indicators**: Clear visual feedback for all states
- **Tooltip support**: Helpful tooltips for all interactive elements

## API Integration

The voice-to-text feature uses the standard Web Speech API with the following configuration:

```javascript
// Language configuration
recognition.lang = selectedLanguage; // e.g., 'hi-IN' for Hindi
recognition.continuous = false;
recognition.interimResults = true;
recognition.maxAlternatives = 1;
```

### Supported Language Codes
```javascript
const supportedLanguages = [
  'en-US', 'en-IN',           // English variants
  'hi-IN', 'bn-IN', 'te-IN', // Major Indian languages
  'mr-IN', 'ta-IN', 'gu-IN', // Regional languages
  'kn-IN', 'ml-IN', 'pa-IN', // South Indian languages
  'or-IN', 'as-IN', 'ur-IN'  // Eastern & Urdu
];
```

## Future Enhancements

### Planned Features
- **Offline support**: Voice recognition without internet
- **Custom vocabulary**: Database-specific term recognition
- **Voice commands**: Navigate the interface using voice
- **Multi-speaker support**: Handle multiple users in one session

### Performance Improvements
- **Faster processing**: Reduced latency for real-time feedback
- **Better accuracy**: Enhanced language models for Indian languages
- **Background noise filtering**: Improved recognition in noisy environments
- **Dialect support**: Better handling of regional accents and dialects

## Support

For technical support or feature requests related to the voice-to-text functionality:

1. **Check browser compatibility** first
2. **Verify microphone permissions** in browser settings
3. **Test with English** to isolate language-specific issues
4. **Check internet connection** for network-related problems

The voice-to-text feature enhances accessibility and user experience by providing a natural way to interact with your database using spoken language in your preferred Indian language or English. 