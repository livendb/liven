"use client";

import { useMemo } from "react";

// ─── Types ────────────────────────────────────────────────────────────────────

export type SupportedLanguage = "txt" | "bash" | "liven";

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

// Rules are ordered: more-specific / longer patterns first.
// Comments and strings always come first so their contents are never re-scanned.

const RULES: Record<SupportedLanguage, TokenRule[]> = {
  bash: [
    { kind: "comment", pattern: /#.*/ },
    { kind: "string", pattern: /"(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'/ },
    {
      kind: "builtin",
      pattern:
        /\b(if|fi|else|elif|then|for|while|do|done|case|esac|function|return|echo|export|set|unset|source|\.|cd|pwd|test|\[\]|declare|local|read|shift|exit|trap|break|continue|true|false)\b/,
    },
    { kind: "keyword", pattern: /\b(then|do|done|fi|esac|elif|else|in)\b/ },
    { kind: "operator", pattern: /&&|\|\||;|\||&|>|<|>>|2>|&>|\|&/ },
    { kind: "builtin", pattern: /\$[a-zA-Z_][a-zA-Z0-9_]*/ },
    { kind: "number", pattern: /\b\d+\b/ },
    { kind: "macro", pattern: /\$\{[^}]*\}/ },
  ],

  liven: [
    { kind: "comment", pattern: /#.*/ },
    { kind: "comment", pattern: /\/\/.*/ },
    { kind: "string", pattern: /"(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'/ },
    {
      kind: "keyword",
      pattern:
        /\b(from|select|where|insert|update|delete|filter|chain|correlate|sequence|limit|order by|group by|having|join|left join|right join|inner join|as|and|or|not|between|in|like|is null|is not null|exists|distinct|count|sum|avg|min|max|upper|lower|trim|ltrim|rtrim|substring|length|concat|coalesce|case|when|then|else|end|cast|extract|date_part|interval|over|partition by|order by|rows|range|current_timestamp|current_date|now)\b/i,
    },
    { kind: "builtin", pattern: /\b(true|false|null)\b/i },
    {
      kind: "type",
      pattern: /\b(string|number|boolean|date|timestamp|json|array)\b/i,
    },
    {
      kind: "operator",
      pattern: /==|!=|<=|>=|<|>|!|\+|-|\*|\/|%|=|\.|,|:|->|=>/,
    },
    { kind: "func", pattern: /\b([a-z_][a-zA-Z0-9_]*)\s*\(/ },
    { kind: "number", pattern: /\b\d+(?:\.\d+)?(?:[eE][+-]?\d+)?\b/ },
    { kind: "macro", pattern: /\$\{[^}]*\}/ },
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
  language = "liven",
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
      className={`group relative rounded-lg overflow-hidden bg-[#F3F1EC] dark:bg-[#282828] border border-border dark:border-zinc-800/80 font-mono text-sm not-prose my-4 ${className}`}
    >
      <pre className="p-4 m-0 overflow-x-auto bg-[#F3F1EC] dark:bg-[#282828] text-[13px] leading-relaxed [tab-2]">
        {lines.map((line, i) => (
          <div key={i} className="flex min-h-[1.65em] whitespace-pre">
            {showLineNumbers && (
              <span
                className="inline-block min-w-8 text-right mr-5 text-[#282828] dark:text-[#504945] select-none text-[11px] pt-[0.1em] flex-0"
                aria-hidden="true"
              >
                {i + 1}
              </span>
            )}
            <code
              className="flex-1 whitespace-nowrap  text-[#3c3836] dark:text-[#ebdbb2]"
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
