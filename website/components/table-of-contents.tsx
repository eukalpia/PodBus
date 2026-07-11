'use client';

import { useEffect, useState } from 'react';

import { ArrowUpRightIcon } from '@/components/icons';
import type { DocPage } from '@/lib/docs-types';

export function TableOfContents({ page }: { page: DocPage }) {
  const [activeId, setActiveId] = useState(page.sections[0]?.id ?? '');

  useEffect(() => {
    const sections = page.sections
      .map((section) => document.getElementById(section.id))
      .filter((section): section is HTMLElement => section !== null);

    if (sections.length === 0) {
      return;
    }

    const updateFromScroll = () => {
      const marker = window.scrollY + 150;
      let current = sections[0].id;
      for (const section of sections) {
        if (section.offsetTop <= marker) {
          current = section.id;
        } else {
          break;
        }
      }
      setActiveId(current);
    };

    updateFromScroll();
    window.addEventListener('scroll', updateFromScroll, { passive: true });
    window.addEventListener('resize', updateFromScroll);

    return () => {
      window.removeEventListener('scroll', updateFromScroll);
      window.removeEventListener('resize', updateFromScroll);
    };
  }, [page.sections]);

  return (
    <aside className="doc-toc" aria-label="On this page">
      <div className="toc-heading">
        <span aria-hidden="true" />
        <strong>On this page</strong>
      </div>
      <nav>
        {page.sections.map((section) => (
          <a
            key={section.id}
            className={activeId === section.id ? 'active' : ''}
            href={`#${section.id}`}
            aria-current={activeId === section.id ? 'location' : undefined}
          >
            {section.title}
          </a>
        ))}
      </nav>
      <div className="toc-divider" />
      <a className="toc-github" href="https://github.com/eukalpia/PodBus/issues/new">
        Report a documentation issue
        <ArrowUpRightIcon />
      </a>
    </aside>
  );
}
