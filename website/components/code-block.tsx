'use client';

import { useState } from 'react';

import { CheckIcon, CopyIcon } from '@/components/icons';

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
        <span>{filename ?? language}</span>
        <button type="button" onClick={copy} aria-label="Copy code">
          {copied ? <CheckIcon /> : <CopyIcon />}
          {copied ? 'Copied' : 'Copy'}
        </button>
      </div>
      <pre data-language={language}>
        <code>{code}</code>
      </pre>
      {caption ? <figcaption>{caption}</figcaption> : null}
    </figure>
  );
}
