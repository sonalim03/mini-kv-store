package protocol

import "testing"

func TestParse_Set(t *testing.T) {
	cmd, err := Parse("SET user:1001 Sonali")
	if err != nil {
		t.Fatal(err)
	}
	if cmd.Type != CmdSet || cmd.Key != "user:1001" || cmd.Value != "Sonali" {
		t.Fatalf("unexpected parse result: %+v", cmd)
	}
}

func TestParse_SetQuotedValue(t *testing.T) {
	cmd, err := Parse(`SET greeting "hello world"`)
	if err != nil {
		t.Fatal(err)
	}
	if cmd.Value != "hello world" {
		t.Fatalf("expected quoted value preserved with space, got %q", cmd.Value)
	}
}

func TestParse_Expire(t *testing.T) {
	cmd, err := Parse("EXPIRE session:123 60")
	if err != nil {
		t.Fatal(err)
	}
	if cmd.Type != CmdExpire || cmd.Sec != 60 {
		t.Fatalf("unexpected parse result: %+v", cmd)
	}
}

func TestParse_MalformedExpire(t *testing.T) {
	if _, err := Parse("EXPIRE session:123 not-a-number"); err != ErrMalformed {
		t.Fatalf("expected ErrMalformed, got %v", err)
	}
}

func TestParse_UnknownCommand(t *testing.T) {
	if _, err := Parse("FOO bar"); err != ErrUnknownCommand {
		t.Fatalf("expected ErrUnknownCommand, got %v", err)
	}
}
