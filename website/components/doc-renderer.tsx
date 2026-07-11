import Link from 'next/link';

import { CodeBlock } from '@/components/code-block';
import {
  ArrowUpRightIcon,
  ChevronRightIcon,
  CheckIcon,
  InfoIcon,
  WarningIcon,
} from '@/components/icons';
import { InlineText } from '@/components/inline-text';
import type { DocBlock, DocPage, NoteTone } from '@/lib/docs-types';

export { TableOfContents } from '@/components/table-of-contents';

function NoteIcon({ tone }: { tone: NoteTone }) {
  if (tone === 'success') {
    return <CheckIcon />;
  }
  if (tone === 'warning' || tone === 'danger') {
    return <WarningIcon />;
  }
  return <InfoIcon />;
}

function Block({ block }: { block: DocBlock }) {
  if (block.type === 'paragraph') {
    return (
      <p>
        <InlineText text={block.text} />
      </p>
    );
  }

  if (block.type === 'bullets') {
    return (
      <ul className="doc-list">
        {block.items.map((item) => (
          <li key={item}>
            <span aria-hidden="true" />
            <p><InlineText text={item} /></p>
          </li>
        ))}
      </ul>
    );
  }

  if (block.type === 'steps') {
    return (
      <ol className="doc-steps">
        {block.items.map((item, index) => (
          <li key={item.title}>
            <span>{String(index + 1).padStart(2, '0')}</span>
            <div>
              <h3>{item.title}</h3>
              <p><InlineText text={item.description} /></p>
            </div>
          </li>
        ))}
      </ol>
    );
  }

  if (block.type === 'code') {
    return (
      <CodeBlock
        code={block.code}
        language={block.language}
        filename={block.filename}
        caption={block.caption}
      />
    );
  }

  if (block.type === 'note') {
    return (
      <aside className={`doc-note ${block.tone}`}>
        <div className="doc-note-icon"><NoteIcon tone={block.tone} /></div>
        <div>
          <strong>{block.title}</strong>
          <p><InlineText text={block.text} /></p>
        </div>
      </aside>
    );
  }

  return (
    <div className="doc-table-wrap" tabIndex={0}>
      <table>
        <thead>
          <tr>
            {block.headers.map((header) => <th key={header}>{header}</th>)}
          </tr>
        </thead>
        <tbody>
          {block.rows.map((row, rowIndex) => (
            <tr key={`${row[0]}-${rowIndex}`}>
              {row.map((cell, cellIndex) => (
                <td key={`${cell}-${cellIndex}`}><InlineText text={cell} /></td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function DocRenderer({
  page,
  previous,
  next,
}: {
  page: DocPage;
  previous?: DocPage;
  next?: DocPage;
}) {
  const sourceUrl = 'https://github.com/eukalpia/PodBus/tree/main/website/content';

  return (
    <>
      <article className="doc-article">
        <header className="doc-header">
          <div className="doc-breadcrumbs">
            <Link href="/docs">Docs</Link>
            <ChevronRightIcon />
            <span>{page.category}</span>
          </div>
          <div className="doc-title-row">
            <div>
              {page.badge ? <span className="doc-badge">{page.badge}</span> : null}
              <h1>{page.title}</h1>
              <p>{page.description}</p>
            </div>
          </div>
        </header>

        <div className="doc-content">
          {page.sections.map((section) => (
            <section key={section.id} id={section.id} className="doc-section">
              <h2>
                <a href={`#${section.id}`}>{section.title}</a>
              </h2>
              <div className="doc-blocks">
                {section.blocks.map((block, index) => (
                  <Block key={`${section.id}-${block.type}-${index}`} block={block} />
                ))}
              </div>
            </section>
          ))}
        </div>

        <footer className="doc-meta-footer">
          <a href={sourceUrl}>
            Edit documentation on GitHub
            <ArrowUpRightIcon />
          </a>
          <span>PodBus {page.badge ?? 'documentation'}</span>
        </footer>
      </article>

      <nav className="doc-pagination" aria-label="Documentation pagination">
        {previous ? (
          <Link href={`/docs/${previous.slug}`}>
            <span>Previous</span>
            <strong>{previous.title}</strong>
          </Link>
        ) : <span />}
        {next ? (
          <Link href={`/docs/${next.slug}`} className="next">
            <span>Next</span>
            <strong>{next.title}</strong>
          </Link>
        ) : <span />}
      </nav>
    </>
  );
}
