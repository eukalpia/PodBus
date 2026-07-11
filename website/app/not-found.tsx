import Link from 'next/link';

import { ChevronRightIcon } from '@/components/icons';

export default function NotFound() {
  return (
    <main id="main-content" className="not-found-page">
      <span>404</span>
      <h1>That documentation page does not exist.</h1>
      <p>The page may have moved, or the link may point to an unreleased guide.</p>
      <div>
        <Link className="primary-button" href="/docs">
          Browse documentation <ChevronRightIcon />
        </Link>
        <Link className="secondary-button" href="/">
          Return home
        </Link>
      </div>
    </main>
  );
}
