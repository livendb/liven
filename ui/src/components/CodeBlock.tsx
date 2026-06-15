"use client";

import { useMemo } from "react";

export type SupportedLanguage =
  | "typescript"
  | "javascript"
  | "rust"
  | "python"
  | "go"
  | "java"
  | "kotlin"
  | "erlang"
  | "toml"
  | "txt";

interface HighlightProps {
  code: string | string[];
  language?: SupportedLanguage;
  showLineNumbers?: boolean;
  className?: string;
}

// ─── Token-stream highlighter ─────────────────────────────────────────────────
//
// Instead of running sequential regexes (which corrupt already-emitted spans),
// we scan left-to-right and greedily match the first rule at each position.
// This guarantees: no double-highlighting, no span-inside-span, no re-processing.

type TokenKind =
  | "comment"
  | "string"
  | "keyword"
  | "type"
  | "func"
  | "number"
  | "operator"
  | "builtin"
  | "decorator"
  | "atom"
  | "macro"
  | "plain";

interface TokenRule {
  kind: TokenKind;
  pattern: RegExp; // must be anchored with \A or use sticky flag + lastIndex
}

function escapeHtml(s: string): string {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

/**
 * Tokenise a single line using an ordered list of rules.
 * Rules are tried left-to-right at each position; first match wins.
 * We use sticky regex (flag `y`) so each attempt starts exactly at `pos`.
 */
function tokenise(
  line: string,
  rules: TokenRule[],
): Array<[TokenKind, string]> {
  // Pre-compile sticky versions of every rule pattern
  const sticky = rules.map((r) => ({
    kind: r.kind,
    re: new RegExp(
      r.pattern.source,
      "y" + r.pattern.flags.replace(/[gy]/g, ""),
    ),
  }));

  const tokens: Array<[TokenKind, string]> = [];
  let pos = 0;

  while (pos < line.length) {
    let matched = false;

    for (const rule of sticky) {
      rule.re.lastIndex = pos;
      const m = rule.re.exec(line);
      if (m) {
        tokens.push([rule.kind, m[0]]);
        pos += m[0].length;
        matched = true;
        break;
      }
    }

    if (!matched) {
      // Consume one character as plain text
      const last = tokens[tokens.length - 1];
      if (last && last[0] === "plain") {
        last[1] += line[pos];
      } else {
        tokens.push(["plain", line[pos]]);
      }
      pos++;
    }
  }

  return tokens;
}

function renderTokens(tokens: Array<[TokenKind, string]>): string {
  return tokens
    .map(([kind, text]) => {
      const safe = escapeHtml(text);
      if (kind === "plain") return safe;

      const colorClass = {
        comment: "text-[#928374] italic",
        string: "text-[#79740e] dark:text-[#b8bb26]",
        keyword: "text-[#9d0006] dark:text-[#fb4934] font-semibold",
        type: "text-[#076678] dark:text-[#83a598] font-semibold",
        func: "text-[#b57614] dark:text-[#fabd2f]",
        number: "text-[#8f3f71] dark:text-[#d3869b]",
        operator: "text-[#3c3836] dark:text-[#ebdbb2]",
        builtin: "text-[#427b58] dark:text-[#8ec07c]",
        decorator: "text-[#af3a03] dark:text-[#fe8019]",
        atom: "text-[#076678] dark:text-[#83a598]",
        macro: "text-[#af3a03] dark:text-[#fe8019] font-semibold",
        plain: "",
      }[kind];

      return `<span class="${colorClass}">${safe}</span>`;
    })
    .join("");
}

// ─── Language rule sets ───────────────────────────────────────────────────────

function makeKeywordRule(words: string[]): TokenRule {
  return {
    kind: "keyword",
    pattern: new RegExp(`\\b(${words.join("|")})\\b`),
  };
}

function makeTypeRule(words: string[]): TokenRule {
  return {
    kind: "type",
    pattern: new RegExp(`\\b(${words.join("|")})\\b`),
  };
}

function makeBuiltinRule(words: string[]): TokenRule {
  return {
    kind: "builtin",
    pattern: new RegExp(`\\b(${words.join("|")})\\b(?=\\()`),
  };
}

// Rules are ordered: more-specific / longer patterns first.
// Comments and strings always come first so their contents are never re-scanned.

const RULES: Record<SupportedLanguage, TokenRule[]> = {
  rust: [
    { kind: "comment", pattern: /\/\/.*/ },
    { kind: "string", pattern: /r#+".*?"+#+|r"[^"]*"|b?"(?:[^"\\]|\\.)*"/ },
    { kind: "string", pattern: /'(?:[^'\\]|\\.)+'/ }, // char literal
    makeKeywordRule([
      "use",
      "let",
      "mut",
      "fn",
      "pub",
      "struct",
      "enum",
      "impl",
      "trait",
      "match",
      "return",
      "if",
      "else",
      "while",
      "for",
      "in",
      "loop",
      "break",
      "continue",
      "type",
      "mod",
      "as",
      "where",
      "unsafe",
      "async",
      "await",
      "move",
      "ref",
      "static",
      "const",
      "extern",
      "crate",
      "self",
      "super",
      "true",
      "false",
      "dyn",
      "box",
    ]),
    { kind: "type", pattern: /\b[A-Z][a-zA-Z0-9_]*\b/ },
    { kind: "func", pattern: /\b([a-z_][a-zA-Z0-9_]*)(?=\s*\()/ },
    { kind: "number", pattern: /\b\d[\d_]*(?:\.\d[\d_]*)?(?:[uif]\d+)?\b/ },
    { kind: "operator", pattern: /=>|->|::|\.\.=|\.\.|[+\-*/%=<>!&|?:~^@]+/ },
  ],

  python: [
    { kind: "comment", pattern: /#.*/ },
    { kind: "string", pattern: /"""[\s\S]*?"""|'''[\s\S]*?'''/ },
    {
      kind: "string",
      pattern: /[fFrRbBuU]{0,2}(?:"(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*')/,
    },
    { kind: "decorator", pattern: /@[a-zA-Z_][a-zA-Z0-9_.]*/ },
    makeKeywordRule([
      "def",
      "class",
      "return",
      "if",
      "elif",
      "else",
      "for",
      "while",
      "try",
      "except",
      "finally",
      "with",
      "as",
      "import",
      "from",
      "pass",
      "break",
      "continue",
      "lambda",
      "yield",
      "assert",
      "raise",
      "del",
      "global",
      "nonlocal",
      "True",
      "False",
      "None",
      "and",
      "or",
      "not",
      "is",
      "in",
      "async",
      "await",
    ]),
    makeBuiltinRule([
      "print",
      "len",
      "range",
      "str",
      "int",
      "float",
      "list",
      "dict",
      "tuple",
      "set",
      "open",
      "input",
      "isinstance",
      "type",
      "super",
      "property",
      "staticmethod",
      "classmethod",
      "enumerate",
      "zip",
      "map",
      "filter",
      "sorted",
      "reversed",
      "hasattr",
      "getattr",
      "setattr",
    ]),
    { kind: "number", pattern: /\b\d+(?:\.\d+)?(?:[eE][+-]?\d+)?\b/ },
    { kind: "operator", pattern: /[+\-*/%=<>!&|?:~^]+/ },
  ],

  typescript: [
    { kind: "comment", pattern: /\/\/.*/ },
    { kind: "comment", pattern: /\/\*[\s\S]*?\*\// },
    { kind: "string", pattern: /`(?:[^`\\]|\\.)*`/ },
    { kind: "string", pattern: /"(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'/ },
    makeKeywordRule([
      "export",
      "import",
      "from",
      "const",
      "let",
      "var",
      "function",
      "return",
      "if",
      "else",
      "for",
      "while",
      "switch",
      "case",
      "break",
      "continue",
      "try",
      "catch",
      "finally",
      "throw",
      "new",
      "class",
      "extends",
      "super",
      "this",
      "static",
      "async",
      "await",
      "of",
      "in",
      "instanceof",
      "typeof",
      "void",
      "null",
      "undefined",
      "true",
      "false",
      "interface",
      "type",
      "enum",
      "implements",
      "abstract",
      "declare",
      "readonly",
      "keyof",
      "as",
      "satisfies",
    ]),
    makeTypeRule([
      "string",
      "number",
      "boolean",
      "any",
      "unknown",
      "never",
      "object",
      "symbol",
      "bigint",
      "void",
      "Promise",
      "Array",
      "Record",
      "Partial",
      "Required",
      "Pick",
      "Omit",
      "Exclude",
      "Extract",
      "NonNullable",
      "ReturnType",
      "InstanceType",
    ]),
    { kind: "type", pattern: /\b[A-Z][a-zA-Z0-9_]*\b/ },
    { kind: "func", pattern: /\b([a-z_][a-zA-Z0-9_]*)(?=\s*\()/ },
    { kind: "number", pattern: /\b\d+(?:\.\d+)?\b/ },
    { kind: "operator", pattern: /=>|[+\-*/%=<>!&|?:~^]+/ },
  ],

  javascript: [
    { kind: "comment", pattern: /\/\/.*/ },
    { kind: "comment", pattern: /\/\*[\s\S]*?\*\// },
    { kind: "string", pattern: /`(?:[^`\\]|\\.)*`/ },
    { kind: "string", pattern: /"(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'/ },
    makeKeywordRule([
      "export",
      "import",
      "from",
      "const",
      "let",
      "var",
      "function",
      "return",
      "if",
      "else",
      "for",
      "while",
      "switch",
      "case",
      "break",
      "continue",
      "try",
      "catch",
      "finally",
      "throw",
      "new",
      "class",
      "extends",
      "super",
      "this",
      "static",
      "async",
      "await",
      "of",
      "in",
      "instanceof",
      "typeof",
      "void",
      "null",
      "undefined",
      "true",
      "false",
    ]),
    { kind: "type", pattern: /\b[A-Z][a-zA-Z0-9_]*\b/ },
    { kind: "func", pattern: /\b([a-z_][a-zA-Z0-9_]*)(?=\s*\()/ },
    { kind: "number", pattern: /\b\d+(?:\.\d+)?\b/ },
    { kind: "operator", pattern: /=>|[+\-*/%=<>!&|?:~^]+/ },
  ],

  go: [
    { kind: "comment", pattern: /\/\/.*/ },
    { kind: "comment", pattern: /\/\*[\s\S]*?\*\// },
    { kind: "string", pattern: /`[^`]*`/ },
    { kind: "string", pattern: /"(?:[^"\\]|\\.)*"/ },
    makeKeywordRule([
      "package",
      "import",
      "func",
      "var",
      "const",
      "type",
      "return",
      "if",
      "else",
      "for",
      "switch",
      "case",
      "default",
      "fallthrough",
      "break",
      "continue",
      "goto",
      "defer",
      "go",
      "range",
      "select",
      "chan",
      "interface",
      "map",
      "struct",
      "true",
      "false",
      "nil",
      "iota",
    ]),
    makeTypeRule([
      "string",
      "int",
      "int8",
      "int16",
      "int32",
      "int64",
      "uint",
      "uint8",
      "uint16",
      "uint32",
      "uint64",
      "float32",
      "float64",
      "complex64",
      "complex128",
      "byte",
      "rune",
      "bool",
      "error",
      "any",
    ]),
    { kind: "func", pattern: /\b([a-zA-Z_][a-zA-Z0-9_]*)(?=\s*\()/ },
    { kind: "number", pattern: /\b\d+(?:\.\d+)?\b/ },
    { kind: "operator", pattern: /:=|<-|[+\-*/%=<>!&|^]+/ },
  ],

  java: [
    { kind: "comment", pattern: /\/\/.*/ },
    { kind: "comment", pattern: /\/\*[\s\S]*?\*\// },
    { kind: "string", pattern: /"(?:[^"\\]|\\.)*"/ },
    makeKeywordRule([
      "public",
      "private",
      "protected",
      "static",
      "final",
      "abstract",
      "class",
      "interface",
      "enum",
      "extends",
      "implements",
      "import",
      "package",
      "return",
      "if",
      "else",
      "for",
      "while",
      "do",
      "switch",
      "case",
      "break",
      "continue",
      "default",
      "try",
      "catch",
      "finally",
      "throw",
      "throws",
      "new",
      "this",
      "super",
      "instanceof",
      "synchronized",
      "volatile",
      "transient",
      "native",
      "true",
      "false",
      "null",
      "var",
      "record",
      "sealed",
      "permits",
    ]),
    makeTypeRule([
      "String",
      "Integer",
      "Double",
      "Float",
      "Long",
      "Short",
      "Byte",
      "Boolean",
      "Character",
      "Object",
      "List",
      "ArrayList",
      "Map",
      "HashMap",
      "Set",
      "HashSet",
      "Optional",
      "Stream",
    ]),
    { kind: "func", pattern: /\b([a-z_][a-zA-Z0-9_]*)(?=\s*\()/ },
    { kind: "number", pattern: /\b\d+(?:\.\d+)?[lLfFdD]?\b/ },
    { kind: "operator", pattern: /[+\-*/%=<>!&|?:~^]+/ },
  ],

  kotlin: [
    { kind: "comment", pattern: /\/\/.*/ },
    { kind: "comment", pattern: /\/\*[\s\S]*?\*\// },
    { kind: "string", pattern: /"""[\s\S]*?"""/ },
    { kind: "string", pattern: /"(?:[^"\\]|\\.)*"/ },
    makeKeywordRule([
      "fun",
      "val",
      "var",
      "class",
      "interface",
      "object",
      "enum",
      "data",
      "sealed",
      "abstract",
      "open",
      "override",
      "final",
      "private",
      "protected",
      "internal",
      "public",
      "import",
      "package",
      "return",
      "if",
      "else",
      "when",
      "for",
      "while",
      "do",
      "break",
      "continue",
      "try",
      "catch",
      "finally",
      "throw",
      "is",
      "as",
      "in",
      "true",
      "false",
      "null",
      "by",
      "companion",
      "init",
      "constructor",
      "inline",
      "reified",
      "suspend",
      "coroutine",
      "lateinit",
    ]),
    { kind: "type", pattern: /\b[A-Z][a-zA-Z0-9_]*\b/ },
    { kind: "func", pattern: /\b([a-z_][a-zA-Z0-9_]*)(?=\s*\()/ },
    { kind: "number", pattern: /\b\d+(?:\.\d+)?[LlFf]?\b/ },
    { kind: "operator", pattern: /->|[+\-*/%=<>!&|?:~^]+/ },
  ],

  erlang: [
    { kind: "comment", pattern: /%.*/ },
    { kind: "string", pattern: /"(?:[^"\\]|\\.)*"/ },
    { kind: "atom", pattern: /:[a-z_][a-zA-Z0-9_]*/ },
    { kind: "macro", pattern: /\?[A-Z_][A-Z0-9_]*/ },
    makeKeywordRule([
      "module",
      "export",
      "import",
      "include",
      "record",
      "define",
      "ifdef",
      "endif",
      "spec",
      "type",
      "fun",
      "case",
      "of",
      "if",
      "when",
      "receive",
      "after",
      "try",
      "catch",
      "throw",
      "let",
      "andalso",
      "orelse",
      "not",
      "div",
      "rem",
      "true",
      "false",
      "ok",
      "error",
      "undefined",
      "begin",
      "end",
      "cond",
      "query",
    ]),
    { kind: "type", pattern: /\b[A-Z][a-zA-Z0-9_]*\b/ },
    { kind: "number", pattern: /\b\d+(?:\.\d+)?\b/ },
    { kind: "operator", pattern: /[+\-*/%=<>!|:~^\\]+/ },
  ],

  toml: [
    { kind: "comment", pattern: /#.*/ },
    { kind: "string", pattern: /"""[\s\S]*?"""/ },
    { kind: "string", pattern: /'''[\s\S]*?'''/ },
    { kind: "string", pattern: /"(?:[^"\\]|\\.)*"|'[^']*'/ },
    { kind: "type", pattern: /^\s*\[+[^\]]*\]+/ }, // [section] or [[array]]
    { kind: "keyword", pattern: /^[a-zA-Z_][a-zA-Z0-9_.]*(?=\s*=)/ },
    { kind: "builtin", pattern: /\b(true|false)\b/ },
    { kind: "number", pattern: /\b\d+(?:\.\d+)?(?:[eE][+-]?\d+)?\b/ },
    { kind: "operator", pattern: /=/ },
  ],

  txt: [], // no highlighting — falls through to plain
};

// ─── Core highlight function ──────────────────────────────────────────────────

function highlightLine(line: string, language: SupportedLanguage): string {
  if (language === "txt") return escapeHtml(line);
  const rules = RULES[language];
  const tokens = tokenise(line, rules);
  return renderTokens(tokens);
}

// ─── Component ────────────────────────────────────────────────────────────────

export default function CodeBlock({
  code,
  language = "rust",
  showLineNumbers = false,
  className = "",
}: HighlightProps) {
  const fullCode = useMemo(
    () => (Array.isArray(code) ? code.join("\n") : code),
    [code],
  );

  const lines = useMemo(() => fullCode.split("\n"), [fullCode]);

  return (
    <div
      className={`group relative rounded-lg overflow-hidden bg-[#F3F1EC] dark:bg-body-bg border border-slate-200/80 dark:border-zinc-800/80 font-mono text-sm not-prose my-4 ${className}`}
    >
      <pre className="p-4 m-0 overflow-x-auto bg-[#F3F1EC] dark:bg-body-bg text-[13px] whitespace-nowrap leading-relaxed [tab-2]">
        {lines.map((line, i) => (
          <div key={i} className="flex min-h-[1.65em] whitespace-nowrap">
            {showLineNumbers && (
              <span
                className="inline-block min-w-8 text-right mr-5 text-[#bdae93] dark:text-[#504945] select-none text-[11px] pt-[0.1em] flex-shrink-0"
                aria-hidden="true"
              >
                {i + 1}
              </span>
            )}
            <code
              className="flex-1 whitespace-nowrap text-[#3c3836] dark:text-[#ebdbb2]"
              dangerouslySetInnerHTML={{
                __html: highlightLine(line, language),
              }}
            />
          </div>
        ))}
      </pre>
    </div>
  );
}
