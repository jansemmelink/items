package items

import (
	"fmt"
	"unicode"
)

func validateIdentifier(s string) error {
	if len(s) < 1 {
		return fmt.Errorf("empty string")
	}
	if !unicode.IsLetter(rune(s[0])) {
		return fmt.Errorf("does not start with a letter")
	}
	//rest must be letters, digits or underscores only
	for _, c := range s {
		if !unicode.IsLetter(c) && !unicode.IsDigit(c) && c != '_' {
			return fmt.Errorf("has non-(letters, digits or underscores) characters")
		}
	}
	return nil
}
