'use client';

import Link from 'next/link';
import { useRouter } from 'next/navigation';
import { useEffect, useMemo, useRef, useState } from 'react';

import { CloseIcon, SearchIcon } from '@/components/icons';

export interface SearchEntry {
  slug: string;
  title: string;
  description: string;
  category: string;
  headings: string[];
  keywords: string[];
}

export function SearchDialog({ entries }: { entries: SearchEntry[] }) {
  const router = useRouter();
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState('');
  const [selectedIndex, setSelectedIndex] = useState(0);
  const inputRef = useRef<HTMLInputElement>(null);
  const resultRefs = useRef<Array<HTMLAnchorElement | null>>([]);

  useEffect(() => {
    function onKeyDown(event: KeyboardEvent) {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'k') {
        event.preventDefault();
        setOpen((value) => !value);
      }
      if (event.key === 'Escape') {
        setOpen(false);
      }
    }

    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, []);

  useEffect(() => {
    if (!open) {
      return;
    }
    const frame = window.requestAnimationFrame(() => inputRef.current?.focus());
    document.body.classList.add('dialog-open');
    return () => {
      window.cancelAnimationFrame(frame);
      document.body.classList.remove('dialog-open');
    };
  }, [open]);

  const results = useMemo(() => {
    const normalized = query.trim().toLowerCase();
    if (!normalized) {
      return entries.slice(0, 10);
    }

    const terms = normalized.split(/\s+/).filter(Boolean);
    return entries
      .map((entry) => {
        const title = entry.title.toLowerCase();
        const description = entry.description.toLowerCase();
        const category = entry.category.toLowerCase();
        const headings = entry.headings.join(' ').toLowerCase();
        const keywords = entry.keywords.join(' ').toLowerCase();
        let score = 0;
        for (const term of terms) {
          if (title === term) score += 20;
          if (title.includes(term)) score += 10;
          if (category.includes(term)) score += 5;
          if (headings.includes(term)) score += 4;
          if (description.includes(term)) score += 3;
          if (keywords.includes(term)) score += 1;
        }
        return { entry, score };
      })
      .filter((item) => item.score > 0)
      .sort((left, right) => right.score - left.score)
      .slice(0, 12)
      .map((item) => item.entry);
  }, [entries, query]);

  useEffect(() => {
    setSelectedIndex(0);
    resultRefs.current = [];
  }, [query, open]);

  useEffect(() => {
    resultRefs.current[selectedIndex]?.scrollIntoView({ block: 'nearest' });
  }, [selectedIndex]);

  function close() {
    setOpen(false);
    setQuery('');
    setSelectedIndex(0);
  }

  function onSearchKeyDown(event: React.KeyboardEvent<HTMLInputElement>) {
    if (results.length === 0) {
      return;
    }

    if (event.key === 'ArrowDown') {
      event.preventDefault();
      setSelectedIndex((index) => (index + 1) % results.length);
    } else if (event.key === 'ArrowUp') {
      event.preventDefault();
      setSelectedIndex((index) => (index - 1 + results.length) % results.length);
    } else if (event.key === 'Enter') {
      event.preventDefault();
      const selected = results[selectedIndex];
      if (selected) {
        close();
        router.push(`/docs/${selected.slug}`);
      }
    }
  }

  return (
    <>
      <button className="search-trigger" type="button" onClick={() => setOpen(true)}>
        <SearchIcon />
        <span>Search documentation</span>
        <kbd>⌘ K</kbd>
      </button>

      {open ? (
        <div className="search-overlay" role="presentation" onMouseDown={close}>
          <section
            className="search-panel"
            role="dialog"
            aria-modal="true"
            aria-label="Search documentation"
            onMouseDown={(event) => event.stopPropagation()}
          >
            <div className="search-input-row">
              <SearchIcon />
              <input
                ref={inputRef}
                value={query}
                onChange={(event) => setQuery(event.target.value)}
                onKeyDown={onSearchKeyDown}
                placeholder="Search guides, APIs, transports…"
                aria-label="Search query"
                aria-activedescendant={
                  results[selectedIndex] ? `search-result-${results[selectedIndex].slug}` : undefined
                }
              />
              <button type="button" onClick={close} aria-label="Close search">
                <CloseIcon />
              </button>
            </div>

            <div className="search-results" role="listbox" aria-label="Search results">
              {results.length > 0 ? (
                results.map((entry, index) => (
                  <Link
                    key={entry.slug}
                    id={`search-result-${entry.slug}`}
                    ref={(element) => {
                      resultRefs.current[index] = element;
                    }}
                    className={selectedIndex === index ? 'selected' : ''}
                    role="option"
                    aria-selected={selectedIndex === index}
                    href={`/docs/${entry.slug}`}
                    onMouseEnter={() => setSelectedIndex(index)}
                    onClick={close}
                  >
                    <span>{entry.category}</span>
                    <strong>{entry.title}</strong>
                    <p>{entry.description}</p>
                  </Link>
                ))
              ) : (
                <div className="search-empty">
                  <strong>No documentation found</strong>
                  <p>Try a transport name, API type, or reliability concept.</p>
                </div>
              )}
            </div>

            <footer className="search-footer">
              <span><kbd>↑</kbd><kbd>↓</kbd> browse</span>
              <span><kbd>↵</kbd> open</span>
              <span><kbd>esc</kbd> close</span>
            </footer>
          </section>
        </div>
      ) : null}
    </>
  );
}
