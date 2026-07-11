'use client';

import { useMemo, useState, type ReactNode } from 'react';

import { CheckIcon, CopyIcon } from '@/components/icons';

type TokenKind =
  | 'comment'
  | 'string'
  | 'keyword'
  | 'type'
  | 'number'
  | 'property'
  | 'function'
  | 'variable'
  | 'operator'
  | 'annotation';

interface TokenPattern {
  kind: TokenKind;
  pattern: RegExp;
}

const dartPatterns: TokenPattern[] = [
  { kind: 'comment', pattern: /\/\/.*$/g },
  { kind: 'string', pattern: /'(?:\\.|[^'\\])*'|"(?:\\.|[^"\\])*"/g },
  { kind: 'annotation', pattern: /@[A-Za-z_]\w*/g },
  {
    kind: 'keyword',
    pattern:
      /\b(?:abstract|as|assert|async|await|break|case|catch|class|const|continue|default|do|else|enum|export|extends|extension|external|factory|false|final|finally|for|if|implements|import|in|interface|is|late|library|mixin|new|null|on|operator|part|required|rethrow|return|sealed|show|static|super|switch|sync|this|throw|true|try|typedef|var|void|when|while|with|yield)\b/g,
  },
  {
    kind: 'type',
    pattern:
      /\b(?:bool|DateTime|DeadLetterPolicy|double|Duration|dynamic|Future|int|Iterable|List|Map|MessageHeaders|MessagingCapability|MessagingCapabilities|MessagingConfig|MessagingLimits|NatsJetStreamJobQueue|NatsMessagingConfig|Never|num|Object|PostgresInbox|PostgresOutbox|PostgresOutboxRelay|RetryPolicy|Set|Stream|String|Uri)\b/g,
  },
  { kind: 'number', pattern: /\b(?:0x[\da-fA-F]+|\d+(?:\.\d+)?)\b/g },
  { kind: 'function', pattern: /\b[A-Za-z_]\w*(?=\s*\()/g },
  { kind: 'operator', pattern: /=>|==|!=|<=|>=|\?\?|\?\.|&&|\|\||[=+\-*\/<>&!?]/g },
];

const jsonPatterns: TokenPattern[] = [
  { kind: 'property', pattern: /"(?:\\.|[^"\\])*"(?=\s*:)/g },
  { kind: 'string', pattern: /"(?:\\.|[^"\\])*"/g },
  { kind: 'keyword', pattern: /\b(?:true|false|null)\b/g },
  { kind: 'number', pattern: /-?\b\d+(?:\.\d+)?(?:e[+-]?\d+)?\b/gi },
];

const yamlPatterns: TokenPattern[] = [
  { kind: 'comment', pattern: /#.*$/g },
  { kind: 'string', pattern: /'(?:\\.|[^'\\])*'|"(?:\\.|[^"\\])*"/g },
  { kind: 'property', pattern: /\b[A-Za-z_][\w.-]*(?=\s*:)/g },
  { kind: 'keyword', pattern: /\b(?:true|false|null|yes|no|on|off)\b/gi },
  { kind: 'number', pattern: /\b\d+(?:\.\d+)?\b/g },
  { kind: 'operator', pattern: /[|>&*-]/g },
];

const shellPatterns: TokenPattern[] = [
  { kind: 'comment', pattern: /#.*$/g },
  { kind: 'string', pattern: /'(?:\\.|[^'\\])*'|"(?:\\.|[^"\\])*"/g },
  { kind: 'variable', pattern: /\$\{?[A-Za-z_]\w*\}?/g },
  { kind: 'property', pattern: /--[A-Za-z][\w-]*/g },
  {
    kind: 'keyword',
    pattern:
      /\b(?:cd|cp|dart|docker|done|else|export|fi|for|git|if|in|mkdir|npm|printf|rm|run|set|test|then|while)\b/g,
  },
  { kind: 'number', pattern: /\b\d+(?:\.\d+)?\b/g },
  { kind: 'operator', pattern: /&&|\|\||[|>&]/g },
];

function patternsFor(language: string): TokenPattern[] {
  const normalized = language.toLowerCase();
  if (['dart', 'typescript', 'ts', 'tsx', 'javascript', 'js', 'jsx'].includes(normalized)) {
    return dartPatterns;
  }
  if (normalized === 'json') {
    return jsonPatterns;
  }
  if (['yaml', 'yml'].includes(normalized)) {
    return yamlPatterns;
  }
  if (['bash', 'shell', 'sh', 'zsh'].includes(normalized)) {
    return shellPatterns;
  }
  return [];
}

function highlightLine(line: string, patterns: TokenPattern[], lineIndex: number): ReactNode[] {
  if (patterns.length === 0 || line.length === 0) {
    return [line || ' '];
  }

  const nodes: ReactNode[] = [];
  let cursor = 0;
  let tokenIndex = 0;

  while (cursor < line.length) {
    let winner:
      | { kind: TokenKind; index: number; value: string; priority: number }
      | undefined;

    patterns.forEach((candidate, priority) => {
      candidate.pattern.lastIndex = cursor;
      const match = candidate.pattern.exec(line);
      if (!match || match[0].length === 0) {
        return;
      }
      if (
        !winner ||
        match.index < winner.index ||
        (match.index === winner.index && priority < winner.priority)
      ) {
        winner = {
          kind: candidate.kind,
          index: match.index,
          value: match[0],
          priority,
        };
      }
    });

    if (!winner) {
      nodes.push(line.slice(cursor));
      break;
    }

    if (winner.index > cursor) {
      nodes.push(line.slice(cursor, winner.index));
    }

    nodes.push(
      <span
        className={`syntax-${winner.kind}`}
        key={`${lineIndex}-${tokenIndex}-${winner.index}`}
      >
        {winner.value}
      </span>,
    );

    cursor = winner.index + winner.value.length;
    tokenIndex += 1;
  }

  return nodes;
}

export function CodeBlock({
  code,
  language,
  filename,
  caption,
}: {
  code: string;
  language: string;
  filename?: string;
  caption?: string;
}) {
  const [copied, setCopied] = useState(false);
  const highlightedLines = useMemo(() => {
    const patterns = patternsFor(language);
    return code.split('\n').map((line, index) => highlightLine(line, patterns, index));
  }, [code, language]);

  async function copy() {
    try {
      await navigator.clipboard.writeText(code);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1600);
    } catch {
      setCopied(false);
    }
  }

  return (
    <figure className="code-frame">
      <div className="code-toolbar">
        <div className="code-toolbar-title">
          <span className="code-window-dots" aria-hidden="true">
            <i />
            <i />
            <i />
          </span>
          <span>{filename ?? language}</span>
        </div>
        <div className="code-toolbar-actions">
          <span className="code-language">{language}</span>
          <button type="button" onClick={copy} aria-label="Copy code">
            {copied ? <CheckIcon /> : <CopyIcon />}
            {copied ? 'Copied' : 'Copy'}
          </button>
        </div>
      </div>
      <pre data-language={language}>
        <code className="code-lines">
          {highlightedLines.map((line, index) => (
            <span className="code-line" key={`${index}-${code.length}`}>
              {line}
            </span>
          ))}
        </code>
      </pre>
      {caption ? <figcaption>{caption}</figcaption> : null}
    </figure>
  );
}
