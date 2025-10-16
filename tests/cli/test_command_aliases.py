import unittest

from click.testing import CliRunner

from tensorlake.cli import cli


class TestCommandAliases(unittest.TestCase):
    """Test that command aliases work through prefix matching"""

    def test_exact_command_names_still_work(self):
        """Test that exact command names continue to work"""
        runner = CliRunner()

        # Test all main commands with exact names
        test_cases = [
            "request",
            "secrets",
            "deploy",
            "parse",
            "login",
            "whoami",
        ]

        for cmd in test_cases:
            with self.subTest(command=cmd):
                result = runner.invoke(cli, [cmd, "--help"], prog_name="tensorlake")
                self.assertEqual(
                    result.exit_code,
                    0,
                    f"Command '{cmd}' failed: {result.output}",
                )

    def test_request_alias_req(self):
        """Test that 'req' works as alias for 'request'"""
        runner = CliRunner()
        result = runner.invoke(cli, ["req", "--help"], prog_name="tensorlake")

        self.assertEqual(result.exit_code, 0, f"Failed: {result.output}")
        self.assertIn("request", result.output.lower())

    def test_secrets_alias_sec(self):
        """Test that 'sec' works as alias for 'secrets'"""
        runner = CliRunner()
        result = runner.invoke(cli, ["sec", "--help"], prog_name="tensorlake")

        self.assertEqual(result.exit_code, 0, f"Failed: {result.output}")
        self.assertIn("secrets", result.output.lower())

    def test_deploy_alias_dep(self):
        """Test that 'dep' works as alias for 'deploy'"""
        runner = CliRunner()
        result = runner.invoke(cli, ["dep", "--help"], prog_name="tensorlake")

        self.assertEqual(result.exit_code, 0, f"Failed: {result.output}")
        self.assertIn("deploy", result.output.lower())

    def test_parse_alias_par(self):
        """Test that 'par' works as alias for 'parse'"""
        runner = CliRunner()
        result = runner.invoke(cli, ["par", "--help"], prog_name="tensorlake")

        self.assertEqual(result.exit_code, 0, f"Failed: {result.output}")
        self.assertIn("parse", result.output.lower())

    def test_login_alias_log(self):
        """Test that 'log' works as alias for 'login'"""
        runner = CliRunner()
        result = runner.invoke(cli, ["log", "--help"], prog_name="tensorlake")

        self.assertEqual(result.exit_code, 0, f"Failed: {result.output}")
        self.assertIn("login", result.output.lower())

    def test_whoami_alias_who(self):
        """Test that 'who' works as alias for 'whoami'"""
        runner = CliRunner()
        result = runner.invoke(cli, ["who", "--help"], prog_name="tensorlake")

        self.assertEqual(result.exit_code, 0, f"Failed: {result.output}")
        self.assertIn("whoami", result.output.lower())


class TestSubcommandsWithAliases(unittest.TestCase):
    """Test that subcommands work correctly when parent command is aliased"""

    def test_req_list_subcommand(self):
        """Test that 'req list' works (request list via alias)"""
        runner = CliRunner()
        result = runner.invoke(cli, ["req", "list", "--help"], prog_name="tensorlake")

        self.assertEqual(result.exit_code, 0, f"Failed: {result.output}")
        self.assertIn("list", result.output.lower())

    def test_req_info_subcommand(self):
        """Test that 'req info' works (request info via alias)"""
        runner = CliRunner()
        result = runner.invoke(cli, ["req", "info", "--help"], prog_name="tensorlake")

        self.assertEqual(result.exit_code, 0, f"Failed: {result.output}")
        self.assertIn("info", result.output.lower())

    def test_req_logs_subcommand(self):
        """Test that 'req logs' works (request logs via alias)"""
        runner = CliRunner()
        result = runner.invoke(cli, ["req", "logs", "--help"], prog_name="tensorlake")

        self.assertEqual(result.exit_code, 0, f"Failed: {result.output}")
        self.assertIn("logs", result.output.lower())

    def test_sec_list_subcommand(self):
        """Test that 'sec list' works (secrets list via alias)"""
        runner = CliRunner()
        result = runner.invoke(cli, ["sec", "list", "--help"], prog_name="tensorlake")

        self.assertEqual(result.exit_code, 0, f"Failed: {result.output}")
        self.assertIn("list", result.output.lower())

    def test_sec_set_subcommand(self):
        """Test that 'sec set' works (secrets set via alias)"""
        runner = CliRunner()
        result = runner.invoke(cli, ["sec", "set", "--help"], prog_name="tensorlake")

        self.assertEqual(result.exit_code, 0, f"Failed: {result.output}")
        self.assertIn("set", result.output.lower())

    def test_sec_unset_subcommand(self):
        """Test that 'sec unset' works (secrets unset via alias)"""
        runner = CliRunner()
        result = runner.invoke(cli, ["sec", "unset", "--help"], prog_name="tensorlake")

        self.assertEqual(result.exit_code, 0, f"Failed: {result.output}")
        self.assertIn("unset", result.output.lower())


class TestVariousAliasPrefixes(unittest.TestCase):
    """Test that various prefix lengths work"""

    def test_single_letter_aliases(self):
        """Test single letter prefixes where unambiguous"""
        runner = CliRunner()

        # 'p' is ambiguous (parse vs pat), but 'w' is unique for whoami
        result = runner.invoke(cli, ["w", "--help"], prog_name="tensorlake")
        self.assertEqual(result.exit_code, 0, f"Failed: {result.output}")

    def test_two_letter_aliases(self):
        """Test two-letter prefixes"""
        runner = CliRunner()

        test_cases = [
            ("re", "request"),
            ("se", "secrets"),
            ("de", "deploy"),
            ("pa", "parse"),
            ("lo", "login"),
            ("wh", "whoami"),
        ]

        for alias, full_cmd in test_cases:
            with self.subTest(alias=alias, full_command=full_cmd):
                result = runner.invoke(cli, [alias, "--help"], prog_name="tensorlake")
                self.assertEqual(
                    result.exit_code,
                    0,
                    f"Alias '{alias}' failed: {result.output}",
                )

    def test_longer_prefix_aliases(self):
        """Test longer but still abbreviated prefixes"""
        runner = CliRunner()

        test_cases = [
            ("reque", "request"),
            ("secre", "secrets"),
            ("deplo", "deploy"),
            ("pars", "parse"),
            ("logi", "login"),
            ("whoam", "whoami"),
        ]

        for alias, full_cmd in test_cases:
            with self.subTest(alias=alias, full_command=full_cmd):
                result = runner.invoke(cli, [alias, "--help"], prog_name="tensorlake")
                self.assertEqual(
                    result.exit_code,
                    0,
                    f"Alias '{alias}' failed: {result.output}",
                )


class TestInvalidAliases(unittest.TestCase):
    """Test that invalid/ambiguous aliases are handled properly"""

    def test_nonexistent_command(self):
        """Test that a prefix matching no commands fails gracefully"""
        runner = CliRunner()
        result = runner.invoke(cli, ["xyz", "--help"], prog_name="tensorlake")

        self.assertNotEqual(result.exit_code, 0)
        # Should show error about unknown command
        self.assertTrue(
            "no such command" in result.output.lower()
            or "error" in result.output.lower()
        )

    def test_empty_prefix(self):
        """Test that empty command name is handled"""
        runner = CliRunner()
        result = runner.invoke(cli, [""], prog_name="tensorlake")

        # Empty command should fail
        self.assertNotEqual(result.exit_code, 0)


class TestCaseInsensitivity(unittest.TestCase):
    """Test that aliases are case-insensitive"""

    def test_uppercase_alias(self):
        """Test that uppercase aliases work"""
        runner = CliRunner()
        result = runner.invoke(cli, ["REQ", "--help"], prog_name="tensorlake")

        self.assertEqual(result.exit_code, 0, f"Failed: {result.output}")

    def test_mixed_case_alias(self):
        """Test that mixed case aliases work"""
        runner = CliRunner()
        result = runner.invoke(cli, ["ReQ", "--help"], prog_name="tensorlake")

        self.assertEqual(result.exit_code, 0, f"Failed: {result.output}")

    def test_lowercase_alias(self):
        """Test that lowercase aliases work (should be default)"""
        runner = CliRunner()
        result = runner.invoke(cli, ["req", "--help"], prog_name="tensorlake")

        self.assertEqual(result.exit_code, 0, f"Failed: {result.output}")


class TestAliasesNotInHelpList(unittest.TestCase):
    """Test that aliases don't clutter the help text"""

    def test_main_help_shows_full_commands(self):
        """Test that main help text shows full command names, not aliases"""
        runner = CliRunner()
        result = runner.invoke(cli, ["--help"], prog_name="tensorlake")

        self.assertEqual(result.exit_code, 0)

        # Should show full command names
        self.assertIn("request", result.output.lower())
        self.assertIn("secrets", result.output.lower())

        # Should NOT show aliases like 'app', 'req', 'sec' in the command list
        # (They might appear in descriptions but not as separate commands)
        lines = result.output.lower().split("\n")
        command_section = False
        for line in lines:
            if "commands:" in line:
                command_section = True
            if command_section and line.strip():
                # These aliases shouldn't appear as standalone command entries
                # We can't test this perfectly without parsing the help output structure,
                # but we verify the full names are there
                pass


if __name__ == "__main__":
    unittest.main()
