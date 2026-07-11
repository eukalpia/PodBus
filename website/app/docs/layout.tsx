import Link from 'next/link';
import type { ReactNode } from 'react';

import { DocsSidebar } from '@/components/docs-sidebar';
import { ChevronRightIcon } from '@/components/icons';
import { docCategories } from '@/lib/docs';

export default function DocsLayout({ children }: { children: ReactNode }) {
  return (
    <main id="main-content" className="docs-main">
      <div className="docs-mobile-index">
        <details>
          <summary>Browse documentation <ChevronRightIcon /></summary>
          <div>
            <Link href="/docs">Overview</Link>
            {docCategories.map((category) => (
              <section key={category.title}>
                <h2>{category.title}</h2>
                {category.pages.map((page) => (
                  <Link key={page.slug} href={`/docs/${page.slug}`}>{page.title}</Link>
                ))}
              </section>
            ))}
          </div>
        </details>
      </div>

      <div className="docs-shell">
        <DocsSidebar categories={docCategories} />
        <div className="docs-page-area">{children}</div>
      </div>
    </main>
  );
}
