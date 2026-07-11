import { withBasePath } from '@/lib/site';

export function BrandLogo({
  className,
  decorative = false,
}: {
  className?: string;
  decorative?: boolean;
}) {
  return (
    <span className={['brand-logo', className].filter(Boolean).join(' ')}>
      <img
        className="brand-logo-dark"
        src={withBasePath('/podbus-wordmark.svg')}
        alt={decorative ? '' : 'PodBus'}
      />
      <img
        className="brand-logo-light"
        src={withBasePath('/podbus-wordmark-light.svg')}
        alt=""
        aria-hidden="true"
      />
    </span>
  );
}
